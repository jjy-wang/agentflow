package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.llm.ChatChunk;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.LlmInvoker;
import cn.com.agentflow.memory.MemoryRetrievalService;
import cn.com.agentflow.telemetry.GenAiTelemetry;
import cn.com.agentflow.tool.ToolExecutor;
import cn.com.agentflow.tool.ToolRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 长期记忆预检索：命中注入 system prompt 独立小节，召回失败降级为无记忆 */
class ReActLoopMemoryTest {

    private LlmInvoker llmInvoker;
    private ContextBuilder contextBuilder;
    private MemoryRetrievalService memoryRetrievalService;
    private ReActLoop loop;

    @BeforeEach
    void setUp() {
        llmInvoker = mock(LlmInvoker.class);
        ToolRegistry toolRegistry = mock(ToolRegistry.class);
        contextBuilder = mock(ContextBuilder.class);
        ContextCompressor contextCompressor = mock(ContextCompressor.class);
        memoryRetrievalService = mock(MemoryRetrievalService.class);

        when(toolRegistry.definitions(any())).thenReturn(List.of());
        when(contextBuilder.build(any(), any(), any()))
                .thenReturn(new ChatRequest("mock-large", List.of(), List.of(), 256, 0.3));
        when(contextCompressor.maybeCompress(any())).thenReturn(Mono.empty());
        when(llmInvoker.invoke(any())).thenReturn(Flux.just(
                new ChatChunk.TextDelta("答复"), new ChatChunk.Usage(3, 1), new ChatChunk.Finished("stop")));

        loop = new ReActLoop(llmInvoker, toolRegistry, mock(ToolExecutor.class), contextBuilder,
                contextCompressor, new LoopGuard(), new OutputProtocolGuard(),
                new AgentFlowProperties(), memoryRetrievalService, GenAiTelemetry.noop());
    }

    private RunContext newContext() {
        RunContext ctx = new RunContext("1", "run-1", 100L, "REACT", "mock-large",
                new RunLimits(12, 8, 60_000, 32_000),
                Sinks.many().multicast().onBackpressureBuffer());
        ctx.append(cn.com.agentflow.llm.ChatMessage.user("上次说的生产库在哪"));
        return ctx;
    }

    @Test
    void 召回命中时把记忆拼进system提示词独立小节() {
        when(memoryRetrievalService.recallSection("1", "上次说的生产库在哪"))
                .thenReturn(Mono.just("\n\n【长期记忆·自动召回】\n1. (0.90) 生产库在华东一区\n"));

        StepVerifier.create(loop.run(newContext(), "sys")).expectNext("答复").verifyComplete();

        ArgumentCaptor<String> prompt = ArgumentCaptor.forClass(String.class);
        verify(contextBuilder).build(any(), any(), prompt.capture());
        assertTrue(prompt.getValue().contains("长期记忆"), "记忆必须作为独立小节注入 system prompt");
        assertTrue(prompt.getValue().contains("生产库在华东一区"));
    }

    @Test
    void 召回失败降级为无记忆_不阻断run() {
        when(memoryRetrievalService.recallSection(any(), any()))
                .thenReturn(Mono.error(new IllegalStateException("pgvector down")));

        StepVerifier.create(loop.run(newContext(), "sys")).expectNext("答复").verifyComplete();

        ArgumentCaptor<String> prompt = ArgumentCaptor.forClass(String.class);
        verify(contextBuilder).build(any(), any(), prompt.capture());
        assertEquals("sys", prompt.getValue(), "召回失败必须按原始提示词继续，不能注入残缺内容");
    }
}
