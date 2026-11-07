package cn.com.agentflow.quota;

import cn.com.agentflow.exception.AgentException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** AIMD 自适应舱壁：加性增（+1/窗口、封顶 max）、乘性减（×0.7、保底 1）、TTFT 超阈值按失败处理 */
class ModelBulkheadAimdTest {

    private final ModelBulkhead bulkhead = new ModelBulkhead();

    private ModelBulkhead.Permit acquire(String model, int max, long ttftThresholdMs) {
        return bulkhead.acquireAdaptive(model, max, ttftThresholdMs).block();
    }

    @Test
    void 初始上限取max的四分之一且至少为1() {
        acquire("m1", 16, 3000).close();
        assertEquals(4, bulkhead.currentLimit("m1"));
        acquire("m2", 2, 3000).close();
        assertEquals(1, bulkhead.currentLimit("m2"));
    }

    @Test
    void 集满一个成功窗口后加性增1() {
        // max=16 → 初始 limit=4，集满 4 个成功构成一个窗口
        for (int i = 0; i < 4; i++) {
            acquire("m", 16, 3000).success(100);
        }
        assertEquals(5, bulkhead.currentLimit("m"), "窗口大小=当前上限，集满后 +1");
        // 新窗口大小为 5，再集满 5 个成功再 +1
        for (int i = 0; i < 5; i++) {
            acquire("m", 16, 3000).success(100);
        }
        assertEquals(6, bulkhead.currentLimit("m"));
    }

    @Test
    void 加性增封顶maxConcurrency() {
        // max=2 → 初始 limit=1，第一个成功（窗口=1）升到 2
        acquire("m", 2, 3000).success(100);
        assertEquals(2, bulkhead.currentLimit("m"));
        // 再集满一个窗口（2 个成功）也不得超过 max
        acquire("m", 2, 3000).success(100);
        acquire("m", 2, 3000).success(100);
        assertEquals(2, bulkhead.currentLimit("m"), "上限不得超过配置 maxConcurrency");
    }

    @Test
    void 失败即乘性减并清空成功窗口() {
        // max=16 → 初始 limit=4，先攒 3 个成功（未满窗口）
        for (int i = 0; i < 3; i++) {
            acquire("m", 16, 3000).success(100);
        }
        acquire("m", 16, 3000).failure();
        assertEquals(2, bulkhead.currentLimit("m"), "4 × 0.7 = 2.8 截断为 2");
        // 失败前攒的 3 个成功被清零：只需 2 个成功即可集满新窗口（窗口=2）
        acquire("m", 16, 3000).success(100);
        acquire("m", 16, 3000).success(100);
        assertEquals(3, bulkhead.currentLimit("m"));
    }

    @Test
    void 乘性减下限为1() {
        // max=2 → 初始 limit=1，连续失败不得跌破 1
        acquire("m", 2, 3000).failure();
        acquire("m", 2, 3000).failure();
        assertEquals(1, bulkhead.currentLimit("m"));
    }

    @Test
    void ttft超阈值按失败处理() {
        // max=16 → 初始 limit=4，阈值 3000ms
        acquire("m", 16, 3000).success(5000);
        assertEquals(2, bulkhead.currentLimit("m"), "TTFT 超阈值等同失败：乘性减");
        acquire("m", 16, 3000).success(1000);
        assertEquals(2, bulkhead.currentLimit("m"), "TTFT 未超阈值只是普通成功，窗口=2 未满");
    }

    @Test
    void 无首包成功不计ttft退化() {
        acquire("m", 16, 3000).success(-1);
        assertEquals(4, bulkhead.currentLimit("m"), "ttftMs=-1 表示无首包，不触发乘性减");
    }

    @Test
    void 占满limit后立刻拒绝不排队() {
        // max=4 → 初始 limit=1，占住唯一名额后第二次获取必须立刻失败
        ModelBulkhead.Permit held = acquire("m", 4, 3000);
        assertThrows(AgentException.class,
                () -> bulkhead.acquireAdaptive("m", 4, 3000).block());
        held.close();
    }

    @Test
    void permit重复关闭幂等不多还名额() {
        // max=4 → limit=1：若 close 幂等失效导致 inFlight 多减，下面的第二次获取就不会被拒绝
        ModelBulkhead.Permit p = acquire("m", 4, 3000);
        p.close();
        p.close();
        ModelBulkhead.Permit first = acquire("m", 4, 3000);
        assertThrows(AgentException.class,
                () -> bulkhead.acquireAdaptive("m", 4, 3000).block());
        first.close();
    }
}
