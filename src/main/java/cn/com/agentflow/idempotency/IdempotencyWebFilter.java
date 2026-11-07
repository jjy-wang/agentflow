package cn.com.agentflow.idempotency;

import cn.com.agentflow.config.AgentFlowProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Idempotency-Key 请求去重。
 *
 * 只作用于 POST /v1/agent/runs 与 POST /v1/agent/runs/stream：
 * - 首次请求：Redis SETNX 写入 IN_PROGRESS（TTL 默认 10 分钟）后放行；
 * - 处理中重复：409 / -32009，不再进入业务，避免重复建 run、重复预扣配额、重复烧 token；
 * - 已完成重复：/runs 有缓存响应时原样重放；SSE 不缓存响应体，只回 409；
 * - 业务异常或 5xx/4xx：CAS 删除锁，客户端可安全重试。
 *
 * 执行顺序排在 ApiKeyAuthWebFilter 之后（见两者的 Order），
 * 因此租户从 Reactor Context 读取，不使用 ThreadLocal。
 */
@Slf4j
@Component
public class IdempotencyWebFilter implements WebFilter, Ordered {

    static final String HEADER_NAME = "Idempotency-Key";

    private static final String RUNS_PATH = "/v1/agent/runs";
    private static final String STREAM_PATH = "/v1/agent/runs/stream";
    private static final String TENANT_CONTEXT_KEY = "tenantId";
    private static final String KEY_PREFIX = "idem:";
    private static final int MAX_CACHED_BODY_BYTES = 64 * 1024;

    private final IdempotencyStore store;
    private final AgentFlowProperties properties;
    private final MeterRegistry registry;

    public IdempotencyWebFilter(IdempotencyStore store,
                                AgentFlowProperties properties,
                                MeterRegistry registry) {
        this.store = store;
        this.properties = properties;
        this.registry = registry;
    }

    /** 晚于 ApiKeyAuthWebFilter（LOWEST_PRECEDENCE - 10），保证租户上下文已写入 */
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE - 5;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        AgentFlowProperties.Idempotency config = properties.getIdempotency();
        if (!config.isEnabled()) {
            return chain.filter(exchange);
        }
        String path = exchange.getRequest().getPath().value();
        boolean stream = STREAM_PATH.equals(path);
        if (!HttpMethod.POST.equals(exchange.getRequest().getMethod()) || (!stream && !RUNS_PATH.equals(path))) {
            return chain.filter(exchange);
        }

        String rawKey = exchange.getRequest().getHeaders().getFirst(HEADER_NAME);
        if (rawKey == null || rawKey.isBlank()) {
            if (!config.isRequireKey()) {
                return chain.filter(exchange);
            }
            return reject(exchange, HttpStatus.BAD_REQUEST, -32003, "缺少 Idempotency-Key 请求头");
        }
        String idempotencyKey = rawKey.trim();
        if (idempotencyKey.length() > config.getKeyMaxLength()) {
            return reject(exchange, HttpStatus.BAD_REQUEST, -32003,
                    "Idempotency-Key 长度超过上限 " + config.getKeyMaxLength() + " 字符");
        }

        boolean cacheable = !stream;
        return Mono.deferContextual(context -> {
            String tenantId = context.getOrDefault(TENANT_CONTEXT_KEY, null);
            if (tenantId == null || tenantId.isBlank()) {
                // 正常链路鉴权过滤器已拦截；走到这里说明上下文异常，按 fail-open 放行不阻断主流程
                log.warn("幂等过滤器缺少租户上下文，降级放行: path={}", path);
                count("fail_open");
                return chain.filter(exchange);
            }
            String redisKey = KEY_PREFIX + tenantId + ":" + idempotencyKey;
            String owner = UUID.randomUUID().toString();
            return acquireAndRun(exchange, chain, redisKey, owner, cacheable, true);
        });
    }

    private Mono<Void> acquireAndRun(ServerWebExchange exchange,
                                     WebFilterChain chain,
                                     String redisKey,
                                     String owner,
                                     boolean cacheable,
                                     boolean allowRetry) {
        Duration ttl = properties.getIdempotency().getTtl();
        return store.tryAcquire(redisKey, owner, ttl)
                .flatMap(outcome -> {
                    if (outcome == IdempotencyStore.AcquireOutcome.ACQUIRED) {
                        count("miss");
                        return runBusiness(exchange, chain, redisKey, owner, cacheable);
                    }
                    if (outcome == IdempotencyStore.AcquireOutcome.UNAVAILABLE) {
                        count("fail_open");
                        log.warn("幂等状态存储不可用，降级放行(fail-open): key={}", redisKey);
                        return chain.filter(exchange);
                    }
                    return handleOccupied(exchange, chain, redisKey, owner, cacheable, allowRetry);
                });
    }

    private Mono<Void> handleOccupied(ServerWebExchange exchange,
                                      WebFilterChain chain,
                                      String redisKey,
                                      String owner,
                                      boolean cacheable,
                                      boolean allowRetry) {
        // 用 Optional 显式区分"没有读到状态"与"处理完成（Mono<Void> 必然为空）"，
        // 否则内层空完成会被外层 switchIfEmpty 误判为无状态，导致同一请求重复执行。
        return store.read(redisKey)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMap(state -> {
                    if (state.isPresent()) {
                        return handleExistingState(exchange, state.get(), cacheable);
                    }
                    // 抢锁失败但状态恰好过期/被淘汰：允许再竞争一次，仍失败则按重复处理
                    if (!allowRetry) {
                        count("conflict");
                        return conflict(exchange, "重复请求正在处理中");
                    }
                    return acquireAndRun(exchange, chain, redisKey, owner, cacheable, false);
                });
    }

    private Mono<Void> handleExistingState(ServerWebExchange exchange,
                                           IdempotencyState state,
                                           boolean cacheable) {
        // 缓存响应只允许还给同一类接口；跨接口复用同一 key 一律按冲突处理
        if (state.kind() == IdempotencyState.Kind.CACHED && cacheable) {
            count("hit");
            return replay(exchange, state.httpStatus(), state.body());
        }
        count("conflict");
        if (state.kind() == IdempotencyState.Kind.COMPLETED) {
            return conflict(exchange, "重复请求已完成，请勿重复提交");
        }
        return conflict(exchange, "重复请求正在处理中");
    }

    private Mono<Void> runBusiness(ServerWebExchange exchange,
                                   WebFilterChain chain,
                                   String redisKey,
                                   String owner,
                                   boolean cacheable) {
        Duration ttl = properties.getIdempotency().getTtl();
        Mono<Void> execution;
        if (cacheable) {
            IdempotencyResponseCapture capture =
                    new IdempotencyResponseCapture(exchange.getResponse(), MAX_CACHED_BODY_BYTES);
            ServerWebExchange mutated = exchange.mutate().response(capture).build();
            execution = chain.filter(mutated)
                    // 收尾挂在响应完成链路上，而不是 filter 的同步 finally
                    .then(Mono.defer(() -> completeWithCapturedBody(capture, redisKey, owner, ttl)));
        } else {
            execution = chain.filter(exchange)
                    .then(Mono.defer(() -> completeStream(redisKey, owner, ttl)));
        }
        return execution.onErrorResume(e -> releaseAndPropagate(redisKey, owner, e));
    }

    private Mono<Void> completeWithCapturedBody(IdempotencyResponseCapture capture,
                                                String redisKey,
                                                String owner,
                                                Duration ttl) {
        HttpStatusCode statusCode = capture.getStatusCode();
        int status = statusCode == null ? HttpStatus.OK.value() : statusCode.value();
        String body = capture.body();
        if (status >= 200 && status < 300 && body != null && !body.isEmpty()) {
            return store.storeResponse(redisKey, owner, status, body, ttl);
        }
        if (status >= 400) {
            // 4xx/5xx 视为本次提交失败：释放锁，允许客户端修正后重试
            count("released");
            return store.release(redisKey, owner);
        }
        // 2xx 但无响应体 / 响应超长：不缓存响应体，仅保留完成标记，避免同 key 重复创建
        return store.markCompleted(redisKey, owner, ttl);
    }

    private Mono<Void> completeStream(String redisKey, String owner, Duration ttl) {
        return store.markCompleted(redisKey, owner, ttl)
                .onErrorResume(e -> {
                    log.warn("幂等完成标记写入失败（等 TTL 自然过期）: key={}, err={}", redisKey, e.toString());
                    return Mono.empty();
                });
    }

    private Mono<Void> releaseAndPropagate(String redisKey, String owner, Throwable error) {
        count("released");
        return store.release(redisKey, owner)
                .onErrorResume(e -> {
                    log.warn("幂等锁释放失败（等 TTL 自然过期）: key={}, err={}", redisKey, e.toString());
                    return Mono.empty();
                })
                .then(Mono.<Void>error(error));
    }

    private Mono<Void> replay(ServerWebExchange exchange, int status, String body) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.valueOf(status));
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.getHeaders().set("Idempotency-Replayed", "true");
        DataBuffer buffer = response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8));
        return response.writeWith(Mono.just(buffer));
    }

    private Mono<Void> conflict(ServerWebExchange exchange, String message) {
        return reject(exchange, HttpStatus.CONFLICT, -32009, message);
    }

    private Mono<Void> reject(ServerWebExchange exchange, HttpStatus status, int code, String message) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"code\":" + code + ",\"message\":\"" + message + "\"}";
        DataBuffer buffer = response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8));
        return response.writeWith(Mono.just(buffer));
    }

    /** 标签只放结果枚举，严禁把 idempotencyKey / tenantId 这类高基数值塞进指标 */
    private void count(String result) {
        Counter.builder("idempotency.filter.total")
                .tag("result", result)
                .register(registry)
                .increment();
    }
}
