package cn.com.agentflow.agent;

import cn.com.agentflow.stream.SseEvent;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LoopGuard 防死循环：步数上限 / 重复调用指纹 / token 预算（md 18.1） */
class LoopGuardTest {

    private final LoopGuard guard = new LoopGuard();

    private RunContext newContext(RunLimits limits) {
        return new RunContext("1", "run-1", 100L, "REACT", "mock-large", limits,
                Sinks.many().multicast().onBackpressureBuffer());
    }

    @Test
    void 步数到上限返回STEP_LIMIT() {
        RunContext ctx = newContext(new RunLimits(3, 8, 60_000, 10_000));
        ctx.nextStep();
        ctx.nextStep();
        assertTrue(guard.check(ctx).isEmpty(), "未到上限不应拦截");
        ctx.nextStep();
        Optional<LoopGuard.Violation> v = guard.check(ctx);
        assertTrue(v.isPresent());
        assertEquals("STEP_LIMIT", v.get().code());
    }

    @Test
    void 同一工具同参数调用两次判定LOOP_DETECTED() {
        RunContext ctx = newContext(new RunLimits(10, 20, 60_000, 10_000));
        assertTrue(guard.checkRepeat(ctx, "asset_search", "{\"query\":\"web\"}").isEmpty());
        Optional<LoopGuard.Violation> v = guard.checkRepeat(ctx, "asset_search", "{\"query\":\"web\"}");
        assertTrue(v.isPresent());
        assertEquals("LOOP_DETECTED", v.get().code());
    }

    @Test
    void 参数空格不同仍判定为重复() {
        RunContext ctx = newContext(new RunLimits(10, 20, 60_000, 10_000));
        assertTrue(guard.checkRepeat(ctx, "asset_search", "{\"query\":\"web\"}").isEmpty());
        // normalize 去空白后指纹一致，第二次命中
        Optional<LoopGuard.Violation> v = guard.checkRepeat(ctx, "asset_search", "{ \"query\" : \"web\" }");
        assertTrue(v.isPresent());
        assertEquals("LOOP_DETECTED", v.get().code());
    }

    @Test
    void 不同参数不判定为重复() {
        RunContext ctx = newContext(new RunLimits(10, 20, 60_000, 10_000));
        guard.checkRepeat(ctx, "asset_search", "{\"query\":\"web\"}");
        assertTrue(guard.checkRepeat(ctx, "asset_search", "{\"query\":\"db\"}").isEmpty());
    }

    @Test
    void token超预算返回TOKEN_BUDGET() {
        RunContext ctx = newContext(new RunLimits(10, 20, 60_000, 100));
        ctx.addTokenUsed(100);
        Optional<LoopGuard.Violation> v = guard.check(ctx);
        assertTrue(v.isPresent());
        assertEquals("TOKEN_BUDGET", v.get().code());
    }

    @Test
    void 工具调用次数到上限返回TOOL_LIMIT() {
        RunContext ctx = newContext(new RunLimits(10, 2, 60_000, 10_000));
        ctx.incToolCall();
        ctx.incToolCall();
        Optional<LoopGuard.Violation> v = guard.check(ctx);
        assertTrue(v.isPresent());
        assertEquals("TOOL_LIMIT", v.get().code());
    }

    @Test
    void 墙钟为0立即超时() {
        RunContext ctx = newContext(new RunLimits(10, 20, 0, 10_000));
        Optional<LoopGuard.Violation> v = guard.check(ctx);
        assertTrue(v.isPresent());
        assertEquals("WALL_CLOCK_TIMEOUT", v.get().code());
    }

    @Test
    void 上下文健康时不拦截() {
        RunContext ctx = newContext(new RunLimits(10, 20, 60_000, 10_000));
        assertTrue(guard.check(ctx).isEmpty());
    }

    @Test
    void 并发重复检测不漏_同一指纹只有一个线程通过() throws Exception {
        RunContext ctx = newContext(new RunLimits(10, 20, 60_000, 10_000));
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Optional<LoopGuard.Violation>>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return guard.checkRepeat(ctx, "asset_search", "{\"query\":\"web\"}");
                }));
            }
            start.countDown();
            int passed = 0;
            for (Future<Optional<LoopGuard.Violation>> future : futures) {
                if (future.get(10, TimeUnit.SECONDS).isEmpty()) {
                    passed++;
                }
            }
            assertEquals(1, passed, "同一指纹并发检测只能有一个线程首次通过，其余必须判定重复");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void 并发自增工具计数准确() throws Exception {
        RunContext ctx = newContext(new RunLimits(10, 100_000, 60_000, 10_000));
        int threads = 16;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    for (int j = 0; j < perThread; j++) {
                        ctx.incToolCall();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(threads * perThread, ctx.getToolCallCount(), "并发自增不得丢计数");
    }
}
