package cn.com.agentflow.quota;

import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.exception.ErrorCodes;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** 每模型一个信号量：获取失败立刻拒绝，不排队，防止慢模型拖垮全局 */
@Component
public class ModelBulkhead {

    public interface Permit extends AutoCloseable {
        @Override
        void close();

        /** 成功回传：ttftMs 为首包毫秒（无首包传 -1）；固定模式等价于 close */
        default void success(long ttftMs) {
            close();
        }

        /** 失败回传：超时/5xx 等；固定模式等价于 close */
        default void failure() {
            close();
        }
    }

    /** AIMD 乘性减系数：出现超时/5xx 或 TTFT 超阈值时 limit = limit × 0.7 */
    private static final double DECREASE_FACTOR = 0.7;

    private final Map<String, Semaphore> semaphores = new ConcurrentHashMap<>();

    private final Map<String, AdaptiveState> adaptiveStates = new ConcurrentHashMap<>();

    public Mono<Permit> acquire(String model, int maxConcurrency) {
        Semaphore sem = semaphores.computeIfAbsent(model, k -> new Semaphore(maxConcurrency, true));
        return Mono.fromCallable(() -> sem.tryAcquire(50, TimeUnit.MILLISECONDS))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(ok -> ok
                        ? Mono.just((Permit) sem::release)
                        : Mono.error(new AgentException(ErrorCodes.MODEL_BUSY, "模型繁忙: " + model)));
    }

    /**
     * AIMD 自适应模式：并发上限不写死。
     * 加性增——每集满一个成功窗口（窗口大小 = 当前上限）+1，封顶 maxConcurrency；
     * 乘性减——一次超时/5xx 或 TTFT 超阈值即 ×0.7，下保底 1。
     * 硬上限仍由 maxConcurrency 的信号量兜底，limit 只控制准入闸门。
     */
    public Mono<Permit> acquireAdaptive(String model, int maxConcurrency, long ttftThresholdMs) {
        AdaptiveState state = adaptiveStates.computeIfAbsent(model,
                k -> new AdaptiveState(maxConcurrency, ttftThresholdMs));
        return Mono.defer(() -> {
            // tryEnter 是无阻塞 CAS 闸门，可直接在订阅线程执行；拒绝必须走 error 而非返回 null
            // （Mono.fromCallable 返回 null 会变成 empty，调用方拿不到拒绝信号）
            if (!tryEnter(state)) {
                return Mono.error(new AgentException(ErrorCodes.MODEL_BUSY, "模型繁忙: " + model));
            }
            // 信号量按配置上限申请，limit 闸门已过这里理论上不会耗尽，超时兜底防御
            return Mono.fromCallable(() -> state.sem.tryAcquire(50, TimeUnit.MILLISECONDS))
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(ok -> {
                        if (ok) {
                            return Mono.just((Permit) new AdaptivePermit(state));
                        }
                        state.inFlight.decrementAndGet();
                        return Mono.error(new AgentException(ErrorCodes.MODEL_BUSY, "模型繁忙: " + model));
                    });
        });
    }

    /** 当前 AIMD 上限：供测试与排障观测，模型未走过自适应模式时返回 -1 */
    public int currentLimit(String model) {
        AdaptiveState s = adaptiveStates.get(model);
        return s == null ? -1 : s.limit.get();
    }

    /** 准入闸门：inFlight < limit 才放行，CAS 循环保证不超发 */
    private boolean tryEnter(AdaptiveState s) {
        while (true) {
            int cur = s.inFlight.get();
            if (cur >= s.limit.get()) {
                return false;
            }
            if (s.inFlight.compareAndSet(cur, cur + 1)) {
                return true;
            }
        }
    }

    private void onSuccess(AdaptiveState s, long ttftMs) {
        // TTFT 超阈值视为退化信号，与失败同等处理
        if (ttftMs >= 0 && ttftMs > s.ttftThresholdMs) {
            onFailure(s);
            return;
        }
        if (s.windowSuccess.incrementAndGet() >= s.limit.get()) {
            s.windowSuccess.set(0);
            s.limit.updateAndGet(cur -> Math.min(s.max, cur + 1));
        }
    }

    private void onFailure(AdaptiveState s) {
        s.windowSuccess.set(0);
        s.limit.updateAndGet(cur -> Math.max(1, (int) (cur * DECREASE_FACTOR)));
    }

    /** 单模型的 AIMD 状态；窗口统计允许并发下的轻微误差，方向正确即可 */
    private static final class AdaptiveState {
        final int max;
        final long ttftThresholdMs;
        final Semaphore sem;
        /** 当前动态上限，初始取 max/4 的保守起点（至少 1），随后按窗口爬坡 */
        final AtomicInteger limit;
        final AtomicInteger inFlight = new AtomicInteger(0);
        final AtomicInteger windowSuccess = new AtomicInteger(0);

        AdaptiveState(int max, long ttftThresholdMs) {
            this.max = max;
            this.ttftThresholdMs = ttftThresholdMs;
            this.sem = new Semaphore(max, true);
            this.limit = new AtomicInteger(Math.max(1, max / 4));
        }
    }

    /** 自适应 Permit：close 只放资源，success/failure 先调 AIMD 再放资源；重复调用幂等 */
    private final class AdaptivePermit implements Permit {
        private final AdaptiveState state;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private AdaptivePermit(AdaptiveState state) {
            this.state = state;
        }

        @Override
        public void close() {
            release();
        }

        @Override
        public void success(long ttftMs) {
            if (released.compareAndSet(false, true)) {
                onSuccess(state, ttftMs);
                exit();
            }
        }

        @Override
        public void failure() {
            if (released.compareAndSet(false, true)) {
                onFailure(state);
                exit();
            }
        }

        private void release() {
            if (released.compareAndSet(false, true)) {
                exit();
            }
        }

        private void exit() {
            state.inFlight.decrementAndGet();
            state.sem.release();
        }
    }
}
