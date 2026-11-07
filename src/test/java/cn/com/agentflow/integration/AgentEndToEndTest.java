package cn.com.agentflow.integration;

import com.redis.testcontainers.RedisContainer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 端到端集成测试（md 18.3）：Testcontainers 起真实 PG / Redis / RabbitMQ。
 * 本机 Docker daemon 不可用时整体 Assumptions 跳过，不影响 mvn test 退出码。
 * 说明：完整 HTTP 级 E2E 依赖 docker-compose 中的 mock-llm 镜像构建，本类覆盖
 * 其依赖的三条基础设施链路（状态机 SQL、配额 Lua、事件总线收发）。
 */
class AgentEndToEndTest {

    private static final boolean DOCKER_AVAILABLE = dockerAvailable();

    private static boolean dockerAvailable() {
        try {
            boolean ok = DockerClientFactory.instance().isDockerAvailable();
            System.err.println("[AgentEndToEndTest] dockerAvailable=" + ok
                    + " DOCKER_HOST=" + System.getenv("DOCKER_HOST"));
            return ok;
        } catch (Throwable t) {
            System.err.println("[AgentEndToEndTest] Docker 不可用: " + t);
            t.printStackTrace();
            return false;
        }
    }

    @BeforeAll
    static void assumeDocker() {
        Assumptions.assumeTrue(DOCKER_AVAILABLE, "本机 Docker daemon 不可用，跳过 Testcontainers 集成测试");
    }

    /** PG：Flyway 迁移后，两个并发事务抢占同一过期任务，SKIP LOCKED 保证只有一个拿到 */
    @Test
    void pg_状态机抢占_双实例只有一方成功() throws Exception {
        try (PostgreSQLContainer<?> pg = new PostgreSQLContainer<>(
                DockerImageName.parse("pgvector/pgvector:pg16"))) {
            pg.start();
            Flyway.configure()
                    .dataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())
                    .load()
                    .migrate();

            String insert = "INSERT INTO t_agent_run(run_id, tenant_id, session_id, mode, state, query,"
                    + " max_steps, token_budget) VALUES ('e2e-run-1', '1', 100, 'REACT', 'PENDING', '压测', 12, 32000)";
            try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
                 Statement st = c.createStatement()) {
                st.execute(insert);
            }

            // 与 RunMapper.claimExpired 相同的抢占 SQL
            String claimSql = "UPDATE t_agent_run SET lease_owner = ?, "
                    + "lease_until = now() + (? || ' second')::interval "
                    + "WHERE run_id IN (SELECT run_id FROM t_agent_run "
                    + "WHERE state IN ('PENDING','RUNNING') AND (lease_until IS NULL OR lease_until < now()) "
                    + "ORDER BY started_at LIMIT ? FOR UPDATE SKIP LOCKED) RETURNING run_id";

            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch fire = new CountDownLatch(1);
            CountDownLatch executed = new CountDownLatch(2);
            try {
                java.util.concurrent.Callable<Integer> claimTask = () -> {
                    int rows;
                    try (Connection c = DriverManager.getConnection(
                            pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
                        c.setAutoCommit(false);
                        try (PreparedStatement ps = c.prepareStatement(claimSql)) {
                            ps.setString(1, "instance-" + Thread.currentThread().getId());
                            ps.setInt(2, 60);
                            ps.setInt(3, 10);
                            ready.countDown();
                            fire.await(10, TimeUnit.SECONDS);
                            try (ResultSet rs = ps.executeQuery()) {
                                int n = 0;
                                while (rs.next()) {
                                    n++;
                                }
                                rows = n;
                            }
                            // 两个事务都执行完再提交，保证锁重叠
                            executed.countDown();
                            executed.await(10, TimeUnit.SECONDS);
                            c.commit();
                        }
                    }
                    return rows;
                };
                Future<Integer> a = pool.submit(claimTask);
                Future<Integer> b = pool.submit(claimTask);
                ready.await(10, TimeUnit.SECONDS);
                fire.countDown();
                int claimed = a.get(30, TimeUnit.SECONDS) + b.get(30, TimeUnit.SECONDS);
                assertEquals(1, claimed, "SKIP LOCKED 下两个并发实例必须恰好只有一个抢到任务");
            } finally {
                pool.shutdownNow();
            }
        }
    }

    /** Redis：真实执行配额 Lua 脚本，预扣→拒绝→结算退还→再预扣成功 */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void redis_配额脚本_预扣结算回环() {
        try (RedisContainer redis = new RedisContainer(DockerImageName.parse("redis:7.2.4-alpine"))) {
            redis.start();
            LettuceConnectionFactory cf = new LettuceConnectionFactory(redis.getHost(), redis.getFirstMappedPort());
            cf.afterPropertiesSet();
            try {
                StringRedisTemplate tpl = new StringRedisTemplate(cf);
                RedisScript<List> reserve = RedisScript.of(new ClassPathResource("lua/quota_reserve.lua"), List.class);
                RedisScript<Long> settle = RedisScript.of(new ClassPathResource("lua/quota_settle.lua"), Long.class);
                List<String> keys = List.of("quota:rpm:mock", "quota:tpm:mock");

                List<?> first = tpl.execute(reserve, keys, "60", "100", "80");
                assertNotNull(first);
                assertEquals(1L, ((Number) first.get(0)).longValue(), "首次预扣必须成功");

                List<?> second = tpl.execute(reserve, keys, "60", "100", "80");
                assertNotNull(second);
                assertEquals(0L, ((Number) second.get(0)).longValue(), "TPM 桶余量不足必须拒绝");
                assertEquals("TPM_EXCEEDED", String.valueOf(second.get(1)));

                Long refunded = tpl.execute(settle, List.of("quota:tpm:mock"), "80", "100");
                assertNotNull(refunded);

                List<?> third = tpl.execute(reserve, keys, "60", "100", "80");
                assertNotNull(third);
                assertEquals(1L, ((Number) third.get(0)).longValue(), "结算退还后必须能再次预扣");
            } finally {
                cf.destroy();
            }
        }
    }

    /** RabbitMQ：事件总线声明队列→发布→消费回环 */
    @Test
    void rabbitmq_事件总线_发布消费回环() {
        try (RabbitMQContainer mq = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-alpine"))) {
            mq.start();
            CachingConnectionFactory cf = new CachingConnectionFactory(mq.getHost(), mq.getAmqpPort());
            try {
                RabbitAdmin admin = new RabbitAdmin(cf);
                Queue queue = new Queue("agent.events.e2e", false, false, true);
                admin.declareQueue(queue);
                RabbitTemplate tpl = new RabbitTemplate(cf);
                String payload = "{\"type\":\"run.completed\",\"runId\":\"e2e-run-1\"}";
                tpl.convertAndSend(queue.getName(), payload);
                Object received = tpl.receiveAndConvert(queue.getName(), 5000);
                assertEquals(payload, received, "事件总线发布/消费必须保持报文一致");
            } finally {
                cf.destroy();
            }
        }
    }
}
