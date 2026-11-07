package cn.com.agentflow.tools;

import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.agent.RunLimits;
import cn.com.agentflow.cache.EmbeddingCache;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.memory.EmbeddingService;
import cn.com.agentflow.memory.VectorMemory;
import cn.com.agentflow.tool.ToolResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** memory_recall 工具：空记忆与命中两条路径，租户来自 RunContext */
class MemoryToolsTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final float[] vector = new float[]{0.1f, 0.2f, 0.3f};

    private EmbeddingService embeddingService;
    private VectorMemory vectorMemory;
    private MemoryTools tools;
    private RunContext ctx;

    @BeforeEach
    void setUp() {
        embeddingService = mock(EmbeddingService.class);
        vectorMemory = mock(VectorMemory.class);
        EmbeddingCache embeddingCache = mock(EmbeddingCache.class);
        AgentFlowProperties props = new AgentFlowProperties();
        tools = new MemoryTools(embeddingService, vectorMemory, embeddingCache, props, mapper);

        when(embeddingCache.get(anyString())).thenReturn(Mono.empty());
        when(embeddingService.embed(anyString())).thenReturn(Mono.just(vector));
        when(embeddingCache.put(anyString(), any(float[].class))).thenReturn(Mono.empty());

        ctx = new RunContext("1", "run-1", 100L, "REACT", "mock-large",
                new RunLimits(12, 8, 60_000, 32_000),
                Sinks.many().multicast().onBackpressureBuffer());
    }

    @Test
    void 空记忆返回成功且条数为0() {
        when(vectorMemory.recall(eq(1L), any(float[].class), eq(5))).thenReturn(Mono.just(List.of()));

        ToolResult result = tools.recall("从未记录过的问题", null, ctx);

        assertTrue(result.success(), "空记忆不是错误，必须返回成功让模型继续推理");
        assertEquals(0, result.data().get("count").asInt());
        assertEquals(0, result.data().get("items").size());
    }

    @Test
    void 命中时按相似度返回记忆内容() {
        when(vectorMemory.recall(eq(1L), any(float[].class), eq(5))).thenReturn(Mono.just(List.of(
                new VectorMemory.MemoryItem(11L, "用户偏好深色主题", 0.91, 0.89),
                new VectorMemory.MemoryItem(12L, "生产库在华东一区", 0.83, 0.80))));

        ToolResult result = tools.recall("用户界面偏好", null, ctx);

        assertTrue(result.success());
        assertEquals(2, result.data().get("count").asInt());
        assertEquals("用户偏好深色主题", result.data().get("items").get(0).get("content").asText());
        assertEquals(0.91, result.data().get("items").get(0).get("similarity").asDouble(), 0.001);
    }
}
