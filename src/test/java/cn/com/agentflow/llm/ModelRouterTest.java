package cn.com.agentflow.llm;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.metric.AgentMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** ModelRouter：指定模型优先，未知模型回退默认，候选列表含兜底 */
class ModelRouterTest {

    private ModelRegistry registry;
    private ModelRouter router;

    @BeforeEach
    void setUp() {
        AgentFlowProperties props = new AgentFlowProperties();
        props.getLlm().setDefaultModel("mock-large");
        props.getLlm().getModels().put("mock-large", new AgentFlowProperties.Model());
        props.getLlm().getModels().put("backup", new AgentFlowProperties.Model());

        registry = mock(ModelRegistry.class);
        when(registry.contains(anyString()))
                .thenAnswer(inv -> List.of("mock-large", "backup").contains(inv.getArgument(0)));
        ModelPort port = mock(ModelPort.class);
        when(registry.port(anyString())).thenReturn(port);
        when(registry.config(anyString())).thenReturn(new AgentFlowProperties.Model());

        router = new ModelRouter(registry, props, mock(AgentMetrics.class), new SimpleMeterRegistry());
    }

    @Test
    void 指定已注册模型时首选该模型() {
        List<ModelRouter.RouteTarget> candidates = router.candidates("backup");
        assertEquals("backup", candidates.get(0).modelName());
    }

    @Test
    void 指定未注册模型时回退默认模型() {
        List<ModelRouter.RouteTarget> candidates = router.candidates("not-exists");
        assertEquals("mock-large", candidates.get(0).modelName());
    }

    @Test
    void 未指定模型时使用默认模型() {
        assertEquals("mock-large", router.candidates(null).get(0).modelName());
    }

    @Test
    void 候选列表含兜底模型且不重复() {
        List<ModelRouter.RouteTarget> candidates = router.candidates("backup");
        assertEquals(2, candidates.size());
        assertEquals("backup", candidates.get(0).modelName());
        assertEquals("mock-large", candidates.get(1).modelName(), "兜底取默认模型之外的第一个");
    }
}
