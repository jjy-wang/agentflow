package cn.com.agentflow.auth;

import cn.com.agentflow.exception.AgentException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** TenantContext：租户随 Reactor Context 传递（WebFlux 下禁止 ThreadLocal） */
class TenantContextTest {

    @Test
    void withTenant写入后current可读() {
        StepVerifier.create(TenantContext.withTenant("42", TenantContext.current()))
                .expectNext("42")
                .verifyComplete();
    }

    @Test
    void current可在链式调用中传递() {
        Mono<String> flow = TenantContext.current().flatMap(tid -> Mono.just("tenant:" + tid));
        StepVerifier.create(TenantContext.withTenant("7", flow))
                .expectNext("tenant:7")
                .verifyComplete();
    }

    @Test
    void 缺少上下文时current报UNAUTHORIZED() {
        StepVerifier.create(TenantContext.current())
                .expectErrorSatisfies(e -> {
                    AgentException ae = assertInstanceOf(AgentException.class, e);
                    assertEquals("UNAUTHORIZED", ae.getCode());
                })
                .verify();
    }

    @Test
    void 不同租户上下文互相隔离() {
        Mono<String> a = TenantContext.withTenant("tenant-a", TenantContext.current());
        Mono<String> b = TenantContext.withTenant("tenant-b", TenantContext.current());
        StepVerifier.create(Mono.zip(a, b))
                .assertNext(t -> {
                    assertEquals("tenant-a", t.getT1());
                    assertEquals("tenant-b", t.getT2());
                })
                .verifyComplete();
    }
}
