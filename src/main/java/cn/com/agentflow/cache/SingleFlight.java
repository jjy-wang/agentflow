package cn.com.agentflow.cache;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 单飞合并：同一 key 的并发加载只执行一次，其余订阅者共享同一个 Mono 结果。
 * doFinally 在源终止（含全体订阅者取消）时把条目从登记表移除，允许下次重新加载；
 * .cache() 保证结果信号对迟到订阅者可重放。
 */
@Component
public class SingleFlight {

    private final ConcurrentHashMap<String, Mono<?>> flights = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> Mono<T> load(String key, Function<String, Mono<T>> loader) {
        return (Mono<T>) flights.computeIfAbsent(key, k -> loader.apply(k)
                .doFinally(sig -> flights.remove(k))
                .cache());
    }
}
