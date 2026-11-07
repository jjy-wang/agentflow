package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.llm.ChatChunk;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.LlmInvoker;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.memory.MemoryRetrievalService;
import cn.com.agentflow.stream.EventType;
import cn.com.agentflow.stream.SseEvent;
import cn.com.agentflow.telemetry.GenAiTelemetry;
import cn.com.agentflow.tool.ToolExecutor;
import cn.com.agentflow.tool.ToolRegistry;
import cn.com.agentflow.tool.ToolResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * HITL 挂起语义：审批预判命中后整批挂起（正常完成信号，不判 TOOL_ALL_FAILED）；
 * 审批通过后只补执行挂起的工具调用，之前已成功的步骤不重跑。
 */
class ReActLoopApprovalTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private LlmInvoker llmInvoker;
    private ToolRegistry toolRegistry;
    private ToolExecutor toolExecutor;
    private ContextBuilder contextBuilder;
    private ContextCompressor contextCompressor;
    private MemoryRetrievalService memoryRetrievalService;
    private AgentFlowProperties props;
    private Sinks.Many<SseEvent> sink;
    private List<SseEvent> events;

    @BeforeEach
    void setUp() {
        llmInvoker = mock(LlmInvoker.class);
        toolRegistry = mock(ToolRegistry.class);
        toolExecutor = mock(ToolExecutor.class);
        contextBuilder = mock(ContextBuilder.class);
        contextCompressor = mock(ContextCompressor.class);
        memoryRetrievalService = mock(MemoryRetrievalService.class);
        props = new AgentFlowProperties();

        when(toolRegistry.definitions(any())).thenReturn(List.of());
        when(contextBuilder.build(any(), any(), any()))
                .thenReturn(new ChatRequest("mock-large", List.of(), List.of(), 256, 0.3));
        when(contextCompressor.maybeCompress(any())).thenReturn(Mono.empty());
        when(memoryRetrievalService.recallSection(any(), any())).thenReturn(Mono.just(""));

        sink = Sinks.many().multicast().onBackpressureBuffer();
        events = new CopyOnWriteArrayList<>();
        sink.asFlux().subscribe(events::add);
    }

    private ReActLoop newLoop() {
        return new ReActLoop(llmInvoker, toolRegistry, toolExecutor, contextBuilder,
                contextCompressor, new LoopGuard(), new OutputProtocolGuard(),
                props, memoryRetrievalService, GenAiTelemetry.noop());
    }

    private RunContext newContext() {
        RunContext ctx = new RunContext("1", "run-approval", 100L, "REACT", "mock-large",
                new RunLimits(12, 20, 60_000, 32_000), sink);
        ctx.append(ChatMessage.user("删除资产"));
        return ctx;
    }

    /** 首轮发起一个高风险工具调用 */
    private Flux<ChatChunk> riskyToolTurn() {
        return Flux.just(
                new ChatChunk.ToolCallDelta(0, "call_risk", "asset_delete", "{\"id\":1}"),
                new ChatChunk.Usage(10, 5),
                new ChatChunk.Finished("tool_calls"));
    }

    @Test
    void 高风险工具触发后是挂起而不是失败() {
        when(toolExecutor.requiresApproval(any())).thenReturn(true);
        when(toolExecutor.awaitApproval(any(), any())).thenReturn(Mono.empty());
        when(llmInvoker.invoke(any())).thenReturn(riskyToolTurn());

        RunContext ctx = newContext();
        StepVerifier.create(newLoop().run(ctx, "system"))
                .verifyComplete();

        assertTrue(ctx.isSuspended(), "命中审批后必须标记挂起");
        assertEquals(1, ctx.getPendingToolCalls().size(), "挂起批次必须持久化待执行调用");
        assertEquals("call_risk", ctx.getPendingToolCalls().get(0).id());
        assertEquals("asset_delete", ctx.getPendingToolCalls().get(0).name());
        verify(toolExecutor, never()).execute(any(), any());
        assertFalse(events.stream().anyMatch(e -> EventType.RUN_FAILED.equals(e.type())),
                "挂起不能走失败收尾");
    }

    @Test
    void 审批通过后只补执行挂起工具一次并继续到成功() {
        ToolCall call = new ToolCall("call_risk", "asset_delete", "{\"id\":1}");
        RunContext ctx = newContext();
        // 上一轮已成功的工具：续跑时只能从历史消息看到结果，绝不能被再次执行
        ctx.append(ChatMessage.assistant("先查询资产",
                List.of(new ToolCall("call_done", "asset_query", "{}"))));
        ctx.append(ChatMessage.tool("call_done", "{\"ok\":true}"));
        // 模拟挂起时已持久化到 state_json 的批次
        ctx.suspend(List.of(call));
        ctx.markSuspended();

        when(toolExecutor.requiresApproval(any())).thenReturn(true);
        when(toolExecutor.execute(any(), any()))
                .thenReturn(Mono.just(ToolResult.ok(mapper.createObjectNode().put("deleted", true))));
        when(llmInvoker.invoke(any())).thenReturn(Flux.just(
                new ChatChunk.TextDelta("已完成"),
                new ChatChunk.Usage(8, 3),
                new ChatChunk.Finished("stop")));

        StepVerifier.create(newLoop().resume(ctx, "system"))
                .expectNext("已完成")
                .verifyComplete();

        ArgumentCaptor<ToolCall> executed = ArgumentCaptor.forClass(ToolCall.class);
        verify(toolExecutor, times(1)).execute(executed.capture(), any());
        assertEquals("call_risk", executed.getValue().id(), "只补执行挂起批次，已成功的 call_done 不重跑");
        verify(toolExecutor, never()).awaitApproval(any(), any());
        assertTrue(ctx.getPendingToolCalls().isEmpty(), "补执行后必须清空挂起批次");
        assertFalse(ctx.isSuspended());
        assertTrue(ctx.getMessages().stream()
                        .anyMatch(m -> "tool".equals(m.role()) && "call_risk".equals(m.toolCallId())),
                "补执行结果必须回灌上下文");
    }
}
