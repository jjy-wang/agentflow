package cn.com.agentflow.llm;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.metric.AgentMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 熔断三态（CLOSED/OPEN/HALF_OPEN）与 fallback 链配置优先级 */
class ModelRouterCooldownTest {

    private AgentFlowProperties props;
    private SimpleMeterRegistry meterRegistry;
    private ModelRouter router;

    @BeforeEach
    void setUp() {
        props = new AgentFlowProperties();
        props.getLlm().setDefaultModel("mock-a");
        props.getLlm().getModels().put("mock-a", new AgentFlowProperties.Model());
        props.getLlm().getModels().put("mock-b", new AgentFlowProperties.Model());
        props.getLlm().getModels().put("mock-c", new AgentFlowProperties.Model());

        AgentFlowProperties.Cooldown cooldown = props.getLlm().getCooldown();
        cooldown.setFailureThreshold(1);
        cooldown.setOpenMs(40);
        cooldown.setHalfOpenProbes(1);

        ModelRegistry registry = mock(ModelRegistry.class);
        when(registry.contains(anyString()))
                .thenAnswer(inv -> props.getLlm().getModels().containsKey(inv.getArgument(0)));
        when(registry.config(anyString()))
                .thenAnswer(inv -> props.getLlm().getModels().get(inv.getArgument(0)));
        when(registry.port(anyString())).thenReturn(mock(ModelPort.class));

        meterRegistry = new SimpleMeterRegistry();
        router = new ModelRouter(registry, props, mock(AgentMetrics.class), meterRegistry);
    }

    @Test
    void 冷却期内跳过该模型() {
        router.onFailure("mock-a");

        assertFalse(router.allowRequest("mock-a"), "冷却期内必须跳过，交由 fallback 链接手");
        assertTrue(router.allowRequest("mock-b"), "其他模型不受影响");
        assertEquals(1.0, meterRegistry.get("llm.model.cooldown.total")
                .tag("model", "mock-a").counter().count(), "冷却打开次数必须可观测");
    }

    @Test
    void 冷却结束后半开探测成功则恢复() throws InterruptedException {
        router.onFailure("mock-a");
        assertFalse(router.allowRequest("mock-a"));

        Thread.sleep(70);
        assertTrue(router.allowRequest("mock-a"), "冷却到期后应放行一个半开探测");
        assertFalse(router.allowRequest("mock-a"), "半开期只放一个探测，其余请求继续跳过");

        router.onSuccess("mock-a");
        assertTrue(router.allowRequest("mock-a"), "探测成功应闭合熔断");
        assertTrue(router.allowRequest("mock-a"), "闭合后恢复全量放行");
    }

    @Test
    void 半开探测失败则重新冷却() throws InterruptedException {
        router.onFailure("mock-a");
        Thread.sleep(70);
        assertTrue(router.allowRequest("mock-a"));

        router.onFailure("mock-a");
        assertFalse(router.allowRequest("mock-a"), "半开探测失败必须重新打开熔断");

        Thread.sleep(70);
        assertTrue(router.allowRequest("mock-a"), "重新冷却结束后可再次半开探测");
    }

    @Test
    void 模型级fallback链优先于全局链() {
        props.getLlm().setFallbackChain(List.of("mock-b"));
        props.getLlm().getModels().get("mock-a").setFallbackChain(List.of("mock-c"));

        assertEquals(List.of("mock-a", "mock-c"), names(router.route("mock-a", null, 0)),
                "显式链以模型级配置为准，不得再追加链外模型");
    }

    @Test
    void 未配置模型链时使用全局fallback链() {
        props.getLlm().setFallbackChain(List.of("mock-c"));

        assertEquals(List.of("mock-a", "mock-c"), names(router.route("mock-a", null, 0)));
    }

    @Test
    void 完全未配置fallback链时保留隐式兜底() {
        assertEquals(List.of("mock-a", "mock-b"), names(router.route("mock-a", null, 0)));
    }

    @Test
    void fallback次数按from与to枚举标签计数() {
        router.recordFallback("mock-a", "mock-b");
        router.recordFallback("mock-a", "mock-b");

        assertEquals(2.0, meterRegistry.get("llm.model.fallback.total")
                .tag("from", "mock-a").tag("to", "mock-b").counter().count());
    }

    private List<String> names(ModelRouter.RoutePlan plan) {
        return plan.candidates().stream()
                .map(ModelRouter.RouteTarget::modelName)
                .collect(Collectors.toList());
    }
}
