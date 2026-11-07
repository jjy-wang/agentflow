package cn.com.agentflow.idempotency;

import cn.com.agentflow.config.AgentFlowProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 幂等过滤器：并发去重、缺失/超长 key、失败释放、Redis 降级、已完成重放 */
class IdempotencyWebFilterTest {

    private static final String RUNS_PATH = "/v1/agent/runs";
    private static final String STREAM_PATH = "/v1/agent/runs/stream";
    private static final String TENANT = "tenant-a";
    private static final String BODY = "{\"code\":0,\"message\":\"OK\",\"data\":\"r-1\"}";

    private AgentFlowProperties properties;
    private InMemoryIdempotencyStore store;
    private IdempotencyWebFilter filter;

    @BeforeEach
    void setUp() {
        properties = new AgentFlowProperties();
        properties.getIdempotency().setTtl(Duration.ofMinutes(10));
        store = new InMemoryIdempotencyStore();
        filter = new IdempotencyWebFilter(store, properties, new SimpleMeterRegistry());
    }

    @Test
    void 并发同key只有首个请求进入业务_其余409() {
        AtomicInteger invoked = new AtomicInteger();
        // 让首个请求保持在处理中，其余请求才能在锁未释放时并发撞上 409
        WebFilterChain chain = exchange -> Mono.delay(Duration.ofMillis(150))
                .then(Mono.defer(() -> {
                    invoked.incrementAndGet();
                    return writeJson(exchange, HttpStatus.OK, BODY);
                }));
        List<MockServerWebExchange> exchanges = IntStream.range(0, 5)
                .mapToObj(i -> exchange(RUNS_PATH, "same-key"))
                .toList();

        StepVerifier.create(Flux.fromIterable(exchanges)
                        .flatMap(ex -> filter.filter(ex, chain).contextWrite(c -> c.put("tenantId", TENANT)))
                        .then())
                .verifyComplete();

        long ok = exchanges.stream()
                .filter(ex -> HttpStatus.OK.equals(ex.getResponse().getStatusCode())).count();
        long conflict = exchanges.stream()
                .filter(ex -> HttpStatus.CONFLICT.equals(ex.getResponse().getStatusCode())).count();
        assertEquals(1, ok, "并发同 key 只能有一个请求进入业务");
        assertEquals(4, conflict, "其余请求必须被 409 挡在业务之前");
        assertEquals(1, invoked.get(), "业务链只允许执行一次");
        String conflictBody = exchanges.stream()
                .filter(ex -> HttpStatus.CONFLICT.equals(ex.getResponse().getStatusCode()))
                .findFirst().orElseThrow()
                .getResponse().getBodyAsString().block();
        assertTrue(conflictBody.contains("-32009"), "409 响应必须携带业务错误码 -32009");
    }

    @Test
    void 缺失key且不强制时放行() {
        AtomicBoolean invoked = new AtomicBoolean();
        MockServerWebExchange exchange = exchange(RUNS_PATH, null);

        StepVerifier.create(filter.filter(exchange, okChain(invoked))
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();

        assertTrue(invoked.get(), "require-key=false 时缺 key 必须保持向后兼容放行");
        assertEquals(HttpStatus.OK, exchange.getResponse().getStatusCode());
        assertEquals(0, store.size());
    }

    @Test
    void 缺失key且强制时返回400() {
        properties.getIdempotency().setRequireKey(true);
        AtomicBoolean invoked = new AtomicBoolean();
        MockServerWebExchange exchange = exchange(RUNS_PATH, null);

        StepVerifier.create(filter.filter(exchange, okChain(invoked))
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();

        assertEquals(HttpStatus.BAD_REQUEST, exchange.getResponse().getStatusCode());
        assertFalse(invoked.get(), "缺少必填 key 不能进入业务");
        String body = exchange.getResponse().getBodyAsString().block();
        assertTrue(body.contains("-32003"), "400 响应应携带参数校验错误码 -32003");
    }

    @Test
    void 超长key返回400且不写存储() {
        properties.getIdempotency().setKeyMaxLength(8);
        AtomicBoolean invoked = new AtomicBoolean();
        MockServerWebExchange exchange = exchange(RUNS_PATH, "0123456789");

        StepVerifier.create(filter.filter(exchange, okChain(invoked))
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();

        assertEquals(HttpStatus.BAD_REQUEST, exchange.getResponse().getStatusCode());
        assertFalse(invoked.get());
        assertEquals(0, store.size(), "超长 key 不应产生任何 Redis 写入");
    }

    @Test
    void 业务返回5xx后释放锁_重试可放行() {
        AtomicInteger invoked = new AtomicInteger();
        WebFilterChain failing = exchange -> {
            invoked.incrementAndGet();
            exchange.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return Mono.empty();
        };
        MockServerWebExchange first = exchange(RUNS_PATH, "retry-key");

        StepVerifier.create(filter.filter(first, failing)
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();
        assertNull(store.raw("idem:" + TENANT + ":retry-key"), "5xx 必须释放幂等锁");

        MockServerWebExchange second = exchange(RUNS_PATH, "retry-key");
        StepVerifier.create(filter.filter(second, okChain(new AtomicBoolean()))
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();
        assertEquals(HttpStatus.OK, second.getResponse().getStatusCode(), "失败后的重试必须能重新进入业务");
        assertEquals(1, invoked.get(), "首次失败请求的业务链应执行过一次");
    }

    @Test
    void 业务异常后释放锁() {
        WebFilterChain broken = exchange -> Mono.error(new IllegalStateException("boom"));
        MockServerWebExchange first = exchange(RUNS_PATH, "error-key");

        StepVerifier.create(filter.filter(first, broken)
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyError(IllegalStateException.class);
        assertNull(store.raw("idem:" + TENANT + ":error-key"), "异常必须先释放锁再把错误抛出去");

        MockServerWebExchange second = exchange(RUNS_PATH, "error-key");
        StepVerifier.create(filter.filter(second, okChain(new AtomicBoolean()))
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();
        assertEquals(HttpStatus.OK, second.getResponse().getStatusCode());
    }

    @Test
    void redis异常时fail_open放行() {
        store.setUnavailable(true);
        AtomicBoolean invoked = new AtomicBoolean();
        MockServerWebExchange exchange = exchange(RUNS_PATH, "redis-down");

        StepVerifier.create(filter.filter(exchange, okChain(invoked))
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();

        assertTrue(invoked.get(), "Redis 不可用时必须 fail-open 而不是把主流程打死");
        assertEquals(HttpStatus.OK, exchange.getResponse().getStatusCode());
    }

    @Test
    void 已完成响应可重放且不重复执行业务() {
        AtomicInteger invoked = new AtomicInteger();
        WebFilterChain chain = exchange -> {
            invoked.incrementAndGet();
            return writeJson(exchange, HttpStatus.OK, BODY);
        };
        MockServerWebExchange first = exchange(RUNS_PATH, "replay-key");
        MockServerWebExchange second = exchange(RUNS_PATH, "replay-key");

        StepVerifier.create(filter.filter(first, chain)
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();
        StepVerifier.create(filter.filter(second, chain)
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();

        assertEquals(1, invoked.get(), "重放不能再次进入业务");
        assertEquals(HttpStatus.OK, second.getResponse().getStatusCode());
        assertEquals(BODY, second.getResponse().getBodyAsString().block());
        assertEquals("true", second.getResponse().getHeaders().getFirst("Idempotency-Replayed"));
    }

    @Test
    void 流式请求完成后同key再次提交返回409() {
        WebFilterChain sse = exchange -> {
            exchange.getResponse().setStatusCode(HttpStatus.OK);
            return Mono.empty();
        };
        MockServerWebExchange first = exchange(STREAM_PATH, "stream-key");
        StepVerifier.create(filter.filter(first, sse)
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();
        assertTrue(store.raw("idem:" + TENANT + ":stream-key").startsWith("COMPLETED|"),
                "SSE 不缓存响应体，但应保留完成标记防重复提交");

        MockServerWebExchange second = exchange(STREAM_PATH, "stream-key");
        StepVerifier.create(filter.filter(second, sse)
                        .contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();
        assertEquals(HttpStatus.CONFLICT, second.getResponse().getStatusCode());
    }

    @Test
    void 非目标路径与GET请求不拦截() {
        AtomicInteger invoked = new AtomicInteger();
        WebFilterChain chain = exchange -> {
            invoked.incrementAndGet();
            return writeJson(exchange, HttpStatus.OK, BODY);
        };
        MockServerWebExchange get = MockServerWebExchange.from(
                MockServerHttpRequest.get(RUNS_PATH).header(IdempotencyWebFilter.HEADER_NAME, "k").build());
        MockServerWebExchange otherPost = MockServerWebExchange.from(
                MockServerHttpRequest.post("/v1/agent/runs/r-1/cancel")
                        .header(IdempotencyWebFilter.HEADER_NAME, "k").build());

        StepVerifier.create(filter.filter(get, chain).contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();
        StepVerifier.create(filter.filter(otherPost, chain).contextWrite(c -> c.put("tenantId", TENANT)))
                .verifyComplete();

        assertEquals(2, invoked.get());
        assertEquals(0, store.size(), "非目标请求不应占用幂等状态");
    }

    private static MockServerWebExchange exchange(String path, String idempotencyKey) {
        MockServerHttpRequest.BodyBuilder builder = MockServerHttpRequest.post(path);
        if (idempotencyKey != null) {
            builder.header(IdempotencyWebFilter.HEADER_NAME, idempotencyKey);
        }
        return MockServerWebExchange.from(builder.build());
    }

    private static WebFilterChain okChain(AtomicBoolean invoked) {
        return exchange -> {
            invoked.set(true);
            return writeJson(exchange, HttpStatus.OK, BODY);
        };
    }

    private static Mono<Void> writeJson(ServerWebExchange exchange, HttpStatus status, String body) {
        exchange.getResponse().setStatusCode(status);
        DataBuffer buffer = exchange.getResponse().bufferFactory()
                .wrap(body.getBytes(StandardCharsets.UTF_8));
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    /** 测试内嵌实现：语义与 Redis 版一致，不依赖容器 */
    private static final class InMemoryIdempotencyStore implements IdempotencyStore {

        private final Map<String, String> values = new ConcurrentHashMap<>();
        private volatile boolean unavailable;

        @Override
        public Mono<AcquireOutcome> tryAcquire(String key, String owner, Duration ttl) {
            if (unavailable) {
                return Mono.just(AcquireOutcome.UNAVAILABLE);
            }
            boolean acquired = values.putIfAbsent(key, inProgress(owner)) == null;
            return Mono.just(acquired ? AcquireOutcome.ACQUIRED : AcquireOutcome.OCCUPIED);
        }

        @Override
        public Mono<IdempotencyState> read(String key) {
            String raw = values.get(key);
            if (raw == null) {
                return Mono.empty();
            }
            if (raw.startsWith("IN_PROGRESS|")) {
                return Mono.just(IdempotencyState.inProgress());
            }
            if (raw.startsWith("COMPLETED|")) {
                return Mono.just(IdempotencyState.completed());
            }
            String[] parts = raw.split("\\|", 4);
            return Mono.just(IdempotencyState.cached(Integer.parseInt(parts[2]),
                    new String(Base64.getDecoder().decode(parts[3]), StandardCharsets.UTF_8)));
        }

        @Override
        public Mono<Void> markCompleted(String key, String owner, Duration ttl) {
            values.computeIfPresent(key, (k, v) -> v.equals(inProgress(owner))
                    ? "COMPLETED|" + owner : v);
            return Mono.empty();
        }

        @Override
        public Mono<Void> storeResponse(String key, String owner, int httpStatus, String body, Duration ttl) {
            String cached = "CACHED|" + owner + "|" + httpStatus + "|"
                    + Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
            values.computeIfPresent(key, (k, v) -> v.equals(inProgress(owner)) ? cached : v);
            return Mono.empty();
        }

        @Override
        public Mono<Void> release(String key, String owner) {
            values.remove(key, inProgress(owner));
            return Mono.empty();
        }

        void setUnavailable(boolean unavailable) {
            this.unavailable = unavailable;
        }

        int size() {
            return values.size();
        }

        String raw(String key) {
            return values.get(key);
        }

        private static String inProgress(String owner) {
            return "IN_PROGRESS|" + owner;
        }
    }
}
