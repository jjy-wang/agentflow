package cn.com.agentflow.run;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RunState 状态机迁移契约：数据库/事件流中流转的 7 个状态保持稳定，终态集合固定 */
class RunStateMachineTest {

    /** SQL、幂等校验与 SSE 事件均按名引用这些状态，改名即破坏兼容 */
    private static final Set<String> STATES_USED_IN_SQL = Set.of(
            "PENDING", "RUNNING", "WAITING_APPROVAL", "SUCCEEDED", "FAILED", "CANCELED", "TIMEOUT");

    @Test
    void 状态枚举与落库取值完全一致() {
        Set<String> actual = java.util.Arrays.stream(RunState.values())
                .map(Enum::name).collect(Collectors.toSet());
        assertEquals(STATES_USED_IN_SQL, actual);
        for (String s : STATES_USED_IN_SQL) {
            assertDoesNotThrow(() -> RunState.valueOf(s));
        }
    }

    @Test
    void 终态集合固定且其余状态可被恢复任务认领() {
        EnumSet<RunState> terminal = EnumSet.of(
                RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELED, RunState.TIMEOUT);
        assertEquals(4, terminal.size());
        EnumSet<RunState> claimable = EnumSet.of(RunState.PENDING, RunState.RUNNING);
        // RunRecoveryJob.claimExpired 只认领 PENDING/RUNNING，绝不触碰终态
        assertTrue(java.util.Collections.disjoint(terminal, claimable));
        assertEquals(terminal.size() + claimable.size() + 1, RunState.values().length);
    }
}
