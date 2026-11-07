package cn.com.agentflow.llm;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.metric.AgentMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 金丝雀灰度路由：租户粘性一致、权重分布大致符合、未命中条件时保持原路由 */
class ModelRouterCanaryTest {

    private AgentFlowProperties props;
    private ModelRouter router;

    @BeforeEach
    void setUp() {
        props = new AgentFlowProperties();
        props.getLlm().setDefaultModel("mock-a");
        props.getLlm().getModels().put("mock-a", new AgentFlowProperties.Model());
        props.getLlm().getModels().put("mock-b", new AgentFlowProperties.Model());
        AgentFlowProperties.Canary canary = props.getLlm().getCanary();
        canary.setEnabled(true);
        canary.setFrom("mock-a");
        canary.setTo("mock-b");
        canary.setWeightPercent(10);
        canary.setStickyByTenant(true);

        ModelRegistry registry = mock(ModelRegistry.class);
        when(registry.contains(anyString()))
                .thenAnswer(inv -> List.of("mock-a", "mock-b").contains(inv.getArgument(0)));
        ModelPort port = mock(ModelPort.class);
        when(registry.port(anyString())).thenReturn(port);
        when(registry.config(anyString())).thenReturn(new AgentFlowProperties.Model());

        router = new ModelRouter(registry, props, mock(AgentMetrics.class), new SimpleMeterRegistry());
    }

    @Test
    void 同租户多次路由结果百分百一致() {
        for (int t = 0; t < 50; t++) {
            String tenantId = "tenant-" + t;
            String first = router.candidates(null, tenantId).get(0).modelName();
            for (int i = 0; i < 100; i++) {
                assertEquals(first, router.candidates(null, tenantId).get(0).modelName(),
                        "粘性路由下同租户多轮对话必须落在同一模型");
            }
        }
    }

    @Test
    void 权重分布大致符合配置百分比() {
        int total = 2000;
        int hits = 0;
        for (int t = 0; t < total; t++) {
            if ("mock-b".equals(router.candidates(null, "tenant-" + t).get(0).modelName())) {
                hits++;
            }
        }
        double ratio = (double) hits / total;
        assertTrue(ratio > 0.05 && ratio < 0.15,
                "weight=10 时命中率应大致落在 10%% 附近，实际=" + ratio);
    }

    @Test
    void 权重100时全部切到新模型() {
        props.getLlm().getCanary().setWeightPercent(100);
        Set<String> seen = new HashSet<>();
        for (int t = 0; t < 200; t++) {
            seen.add(router.candidates(null, "tenant-" + t).get(0).modelName());
        }
        assertEquals(Set.of("mock-b"), seen);
    }

    @Test
    void 权重0或关闭时不做灰度() {
        props.getLlm().getCanary().setWeightPercent(0);
        assertEquals("mock-a", router.candidates(null, "tenant-x").get(0).modelName());
        props.getLlm().getCanary().setWeightPercent(100);
        props.getLlm().getCanary().setEnabled(false);
        assertEquals("mock-a", router.candidates(null, "tenant-x").get(0).modelName());
    }

    @Test
    void 显式指定非from模型不参与灰度() {
        props.getLlm().getCanary().setWeightPercent(100);
        assertEquals("mock-b", router.candidates("mock-b", "tenant-x").get(0).modelName(),
                "首选不是 from 时保持原路由");
    }

    @Test
    void 灰度只替换首选且兜底不重复() {
        props.getLlm().getCanary().setWeightPercent(100);
        var candidates = router.candidates(null, "tenant-x");
        assertEquals("mock-b", candidates.get(0).modelName());
        assertEquals(2, candidates.size());
        assertEquals("mock-a", candidates.get(1).modelName(), "兜底取首选之外的第一个模型");
    }

    @Test
    void 无租户时非粘性随机分桶整体分布仍符合权重() {
        props.getLlm().getCanary().setWeightPercent(20);
        int total = 2000;
        int hits = 0;
        for (int i = 0; i < total; i++) {
            if ("mock-b".equals(router.candidates(null, null).get(0).modelName())) {
                hits++;
            }
        }
        double ratio = (double) hits / total;
        assertTrue(ratio > 0.10 && ratio < 0.30,
                "无租户随机分桶命中率应大致落在 20%% 附近，实际=" + ratio);
    }
}
