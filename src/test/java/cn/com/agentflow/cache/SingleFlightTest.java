package cn.com.agentflow.cache;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** SingleFlight：同 key 并发加载只执行一次源操作，终止后允许重新加载 */
class SingleFlightTest {

    private final SingleFlight singleFlight = new SingleFlight();

    @Test
    void 同key并发加载只执行一次() {
        AtomicInteger counter = new AtomicInteger();
        Function<String, Mono<String>> loader = k -> Mono.fromCallable(() -> {
                    counter.incrementAndGet();
                    return "v-" + k;
                }).delayElement(Duration.ofMillis(50));

        Mono<String> m1 = singleFlight.load("a", loader);
        Mono<String> m2 = singleFlight.load("a", loader);

        StepVerifier.create(Mono.zip(m1, m2))
                .assertNext(t -> assertEquals(t.getT1(), t.getT2(), "两个订阅者必须拿到同一结果"))
                .verifyComplete();
        assertEquals(1, counter.get(), "并发窗口内源操作只能执行一次");
    }

    @Test
    void 不同key各自执行() {
        AtomicInteger counter = new AtomicInteger();
        Function<String, Mono<String>> loader = k -> Mono.fromCallable(() -> {
            counter.incrementAndGet();
            return "v-" + k;
        });
        StepVerifier.create(Mono.zip(singleFlight.load("x", loader), singleFlight.load("y", loader)))
                .assertNext(t -> {
                    assertEquals("v-x", t.getT1());
                    assertEquals("v-y", t.getT2());
                })
                .verifyComplete();
        assertEquals(2, counter.get());
    }

    @Test
    void 源终止后再次加载重新执行() {
        AtomicInteger counter = new AtomicInteger();
        Function<String, Mono<String>> loader = k -> Mono.fromCallable(counter::incrementAndGet).map(i -> "r" + i);
        StepVerifier.create(singleFlight.load("k", loader)).expectNext("r1").verifyComplete();
        StepVerifier.create(singleFlight.load("k", loader)).expectNext("r2").verifyComplete();
        assertEquals(2, counter.get(), "doFinally 移除条目后，下一次加载必须重新执行源操作");
    }
}
