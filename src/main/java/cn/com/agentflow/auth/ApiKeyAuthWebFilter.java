package cn.com.agentflow.auth;

import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;

@Component
@RequiredArgsConstructor
// 显式排在链路尾部并先于幂等过滤器：幂等层必须能读到已解析的租户上下文。
// 不用 HIGHEST_PRECEDENCE，避免抢到 CORS 预检前面把 OPTIONS 直接拒掉。
@Order(Ordered.LOWEST_PRECEDENCE - 10)
public class ApiKeyAuthWebFilter implements WebFilter {

    private final ApiKeyService apiKeyService;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (path.startsWith("/actuator") || path.startsWith("/v3/api-docs") || path.startsWith("/swagger-ui")) {
            return chain.filter(exchange);
        }
        // 控制台静态资源本身不含数据，放行；其内部所有 /v1 调用仍需 X-Api-Key
        if (path.equals("/") || path.equals("/index.html") || path.equals("/favicon.ico")) {
            return chain.filter(exchange);
        }
        String key = exchange.getRequest().getHeaders().getFirst("X-Api-Key");
        if (key == null || key.isBlank()) {
            return reject(exchange, "缺少 X-Api-Key");
        }
        // 查库是阻塞的 → 必须切到 boundedElastic，否则阻塞事件循环线程。
        // 只把 resolveTenant 的异常映射为 401；下游业务异常必须原样抛出，否则会被误报成「鉴权失败」。
        return apiKeyService.resolveTenant(key)
                .subscribeOn(Schedulers.boundedElastic())
                .map(java.util.Optional::of)
                .onErrorResume(e -> Mono.just(java.util.Optional.empty()))
                .flatMap(tenant -> tenant
                        .map(tenantId -> chain.filter(exchange)
                                .contextWrite(c -> c.put("tenantId", tenantId)))
                        .orElseGet(() -> reject(exchange, "鉴权失败")));
    }

    private Mono<Void> reject(ServerWebExchange exchange, String msg) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"code\":-32001,\"message\":\"" + msg + "\"}";
        DataBuffer buffer = exchange.getResponse().bufferFactory()
                .wrap(body.getBytes(StandardCharsets.UTF_8));
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }
}
