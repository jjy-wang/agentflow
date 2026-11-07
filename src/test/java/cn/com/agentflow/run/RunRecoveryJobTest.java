package cn.com.agentflow.run;

import cn.com.agentflow.mapper.RunMapper;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RunRecoveryJob 集成测试（md 18.1）：两个实例同时抢同一批过期任务，
 * 依赖 claimExpired 的 FOR UPDATE SKIP LOCKED，最终只有一个实例拿到。
 * 本机 Docker daemon 不可用时整体跳过。
 */
class RunRecoveryJobTest {

    private static final boolean DOCKER_AVAILABLE = dockerAvailable();

    private static boolean dockerAvailable() {
        try {
            boolean ok = DockerClientFactory.instance().isDockerAvailable();
            System.err.println("[RunRecoveryJobTest] dockerAvailable=" + ok
                    + " DOCKER_HOST=" + System.getenv("DOCKER_HOST"));
            return ok;
        } catch (Throwable t) {
            System.err.println("[RunRecoveryJobTest] Docker 不可用: " + t);
            t.printStackTrace();
            return false;
        }
    }

    @BeforeAll
    static void assumeDocker() {
        Assumptions.assumeTrue(DOCKER_AVAILABLE, "本机 Docker daemon 不可用，跳过 RunRecoveryJob 集成测试");
    }

    @Test
    void 两个实例同时抢只有一个拿到() throws Exception {
        try (PostgreSQLContainer<?> pg = new PostgreSQLContainer<>(
                DockerImageName.parse("pgvector/pgvector:pg16"))) {
            pg.start();
            Flyway.configure()
                    .dataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())
                    .load()
                    .migrate();

            DriverManagerDataSource ds = new DriverManagerDataSource(
                    pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
            Environment env = new Environment("test", new JdbcTransactionFactory(), ds);
            Configuration configuration = new Configuration(env);
            configuration.addMapper(RunMapper.class);
            SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(configuration);

            // 造一条 PENDING 且租约为空（可被认领）的 run
            try (SqlSession s = factory.openSession(true)) {
                RunMapper m = s.getMapper(RunMapper.class);
                m.insert("recover-run-1", "1", 100L, "REACT", "恢复我", 12, 32000);
            }

            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch fire = new CountDownLatch(1);
            CountDownLatch executed = new CountDownLatch(2);
            try {
                Callable<Integer> claim = () -> {
                    int rows;
                    try (SqlSession s = factory.openSession(false)) {
                        RunMapper m = s.getMapper(RunMapper.class);
                        ready.countDown();
                        fire.await(10, TimeUnit.SECONDS);
                        List<Map<String, Object>> claimed = m.claimExpired(
                                "instance-" + Thread.currentThread().getId(), 60, 10);
                        rows = claimed.size();
                        // 两个事务都执行完再提交，保证行锁重叠
                        executed.countDown();
                        executed.await(10, TimeUnit.SECONDS);
                        s.commit();
                    }
                    return rows;
                };
                Future<Integer> a = pool.submit(claim);
                Future<Integer> b = pool.submit(claim);
                ready.await(10, TimeUnit.SECONDS);
                fire.countDown();
                int total = a.get(30, TimeUnit.SECONDS) + b.get(30, TimeUnit.SECONDS);
                assertEquals(1, total, "两个实例同时抢，必须只有一个拿到该 run");
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
