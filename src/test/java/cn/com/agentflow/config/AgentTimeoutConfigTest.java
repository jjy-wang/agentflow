package cn.com.agentflow.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 超时预算配置校验：默认值正确，非法 step 按 max-duration 收敛且不阻断启动 */
class AgentTimeoutConfigTest {

    @Test
    void 默认配置为run5分钟_单步1分钟() {
        AgentFlowProperties props = new AgentFlowProperties();
        assertEquals(300_000, props.getAgent().getMaxDurationMs());
        assertEquals(60_000, props.getAgent().getStepTimeoutMs());

        props.normalizeTimeoutBudget();
        assertEquals(300_000, props.getAgent().getMaxDurationMs(), "合法配置不应被改写");
        assertEquals(60_000, props.getAgent().getStepTimeoutMs(), "合法配置不应被改写");
    }

    @Test
    void 单步预算大于总预算_按总预算收敛() {
        AgentFlowProperties props = new AgentFlowProperties();
        props.getAgent().setMaxDurationMs(1000);
        props.getAgent().setStepTimeoutMs(5000);

        props.normalizeTimeoutBudget();

        assertEquals(1000, props.getAgent().getStepTimeoutMs(), "step 必须收敛为 max-duration");
        assertEquals(1000, props.getAgent().getMaxDurationMs(), "总预算保持不变");
    }

    @Test
    void 单步预算等于总预算_同样收敛不抛异常() {
        AgentFlowProperties props = new AgentFlowProperties();
        props.getAgent().setMaxDurationMs(2000);
        props.getAgent().setStepTimeoutMs(2000);

        props.normalizeTimeoutBudget();

        assertEquals(2000, props.getAgent().getStepTimeoutMs());
    }

    @Test
    void 总预算非正_回退默认值且单步保持合法() {
        AgentFlowProperties props = new AgentFlowProperties();
        props.getAgent().setMaxDurationMs(0);
        props.getAgent().setStepTimeoutMs(0);

        props.normalizeTimeoutBudget();

        assertEquals(300_000, props.getAgent().getMaxDurationMs(), "非法总预算回退默认 5 分钟");
        assertEquals(300_000, props.getAgent().getStepTimeoutMs(), "单步预算按总预算收敛");
    }
}
