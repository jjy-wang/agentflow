package cn.com.agentflow.auth;

import cn.com.agentflow.exception.AgentException;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * WebFlux 下没有 ThreadLocal 语义：租户必须随 Reactor Context 传递。
 * 这是最容易写错的地方——用 ThreadLocal 会在切换线程后丢失。
 * Controller 中取值必须走 current() 的反应式链式写法：
 * TenantContext.current().flatMap(tenantId -> ...)，禁止在方法体同步取。
 */
public final class TenantContext {

    private static final String KEY = "tenantId";

    private TenantContext() {
    }

    public static <T> Mono<T> withTenant(String tenantId, Mono<T> mono) {
        return mono.contextWrite(c -> c.put(KEY, tenantId));
    }

    public static Mono<String> current() {
        return Mono.deferContextual(c -> c.hasKey(KEY)
                ? Mono.just(c.get(KEY))
                : Mono.error(new AgentException("UNAUTHORIZED", "缺少租户上下文")));
    }

    // 注：不提供同步 require()——WebFlux 下同步取租户必然违背反应式语义，统一走 current() 链式。

    public static Function<reactor.util.context.ContextView, String> reader() {
        return c -> c.get(KEY);
    }
}
