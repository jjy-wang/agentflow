package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ChatChunk;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.LlmInvoker;
import cn.com.agentflow.memory.MemoryRetrievalService;
import cn.com.agentflow.stream.SseEvent;
import cn.com.agentflow.telemetry.GenAiTelemetry;
import cn.com.agentflow.tool.ToolExecutor;
import cn.com.agentflow.tool.ToolRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 结构化输出协议回灌重试：非法 JSON 不直接判死，超限才失败；回灌计入步数预算 */
class ReActLoopProtocolRetryTest {

    private LlmInvoker llmInvoker;
    private ReActLoop loop;
    private OutputProtocolGuard guard;
    private List<SseEvent> events;
    private Sinks.Many<SseEvent> sink;

    @BeforeEach
    void setUp() {
        llmInvoker = mock(LlmInvoker.class);
        ToolRegistry toolRegistry = mock(ToolRegistry.class);
        ToolExecutor toolExecutor = mock(ToolExecutor.class);
        ContextBuilder contextBuilder = mock(ContextBuilder.class);
        ContextCompressor contextCompressor = mock(ContextCompressor.class);
        guard = new OutputProtocolGuard();
        MemoryRetrievalService memoryRetrievalService = mock(MemoryRetrievalService.class);
        when(memoryRetrievalService.recallSection(any(), any())).thenReturn(Mono.just(""));
        loop = new ReActLoop(llmInvoker, toolRegistry, toolExecutor, contextBuilder,
                contextCompressor, new LoopGuard(), guard, new AgentFlowProperties(), memoryRetrievalService,
                GenAiTelemetry.noop());

        when(toolRegistry.definitions(any())).thenReturn(List.of());
        when(contextBuilder.build(any(), any(), any()))
                .thenReturn(new ChatRequest("mock-large", List.of(), List.of(), 256, 0.3));
        when(contextCompressor.maybeCompress(any())).thenReturn(Mono.empty());

        sink = Sinks.many().multicast().onBackpressureBuffer();
        events = new CopyOnWriteArrayList<>();
        sink.asFlux().subscribe(events::add);
    }

    private RunContext newContext() {
        return new RunContext("1", "run-1", 100L, "REACT", "mock-large",
                new RunLimits(12, 8, 60_000, 32_000), sink);
    }

    /** 非法输出：工具参数是截断的半截 JSON */
    private Flux<ChatChunk> invalidJsonTurn() {
        return Flux.just(
                new ChatChunk.ToolCallDelta(0, "call_1", "asset_search", "{\"keyword\":"),
                new ChatChunk.Usage(10, 5),
                new ChatChunk.Finished("tool_calls"));
    }

    private Flux<ChatChunk> textTurn(String text) {
        return Flux.just(
                new ChatChunk.TextDelta(text),
                new ChatChunk.Usage(8, 4),
                new ChatChunk.Finished("stop"));
    }

    @Test
    void 第一次非法第二次合法_回灌后恢复且步数受控() {
        when(llmInvoker.invoke(any())).thenAnswer(inv -> invalidJsonTurn())
                .thenAnswer(inv -> textTurn("最终答案"));

        RunContext ctx = newContext();
        StepVerifier.create(loop.run(ctx, "sys"))
                .expectNext("最终答案")
                .verifyComplete();

        assertEquals(1, ctx.getProtocolRetries(), "恰好回灌重试 1 次");
        assertEquals(2, ctx.getStep(), "首轮 + 回灌重试轮共 2 步，必须计入步数预算");
        assertTrue(events.stream().anyMatch(e -> e.type().equals("step.started")
                        && e.data().contains("PROTOCOL_RETRY")),
                "必须发出 PROTOCOL_RETRY 事件");
        assertTrue(ctx.getMessages().stream().anyMatch(m -> "system".equals(m.role())
                        && m.content() != null && m.content().contains("输出协议")),
                "回灌的系统反馈必须包含协议说明");
    }

    @Test
    void 连续三次非法_超过两次回灌判失败() {
        when(llmInvoker.invoke(any())).thenAnswer(inv -> invalidJsonTurn());

        RunContext ctx = newContext();
        StepVerifier.create(loop.run(ctx, "sys"))
                .expectErrorMatches(e -> e instanceof AgentException ae
                        && "PROTOCOL_INVALID".equals(ae.getCode()))
                .verify();

        assertEquals(2, ctx.getProtocolRetries(), "最多回灌 2 次");
        assertEquals(3, ctx.getStep(), "3 轮输出各计 1 步");
    }

    @Test
    void 非法输出不落toolCalls形态_避免上下文配对污染() {
        when(llmInvoker.invoke(any())).thenAnswer(inv -> invalidJsonTurn())
                .thenAnswer(inv -> textTurn("恢复"));

        RunContext ctx = newContext();
        StepVerifier.create(loop.run(ctx, "sys")).expectNext("恢复").verifyComplete();

        assertTrue(ctx.getMessages().stream()
                        .filter(m -> "assistant".equals(m.role()))
                        .noneMatch(m -> m.toolCalls() != null && !m.toolCalls().isEmpty()),
                "协议违规的 assistant 消息只能以纯文本留档");
    }

    @Test
    void 合法工具调用与纯文本均不拦截() {
        assertTrue(guard.validate(LlmTurn.merge(List.of(
                new ChatChunk.ToolCallDelta(0, "call_1", "asset_search", "{\"keyword\":\"a\"}"),
                new ChatChunk.Finished("tool_calls")))).isEmpty());
        assertTrue(guard.validate(LlmTurn.merge(List.of(
                new ChatChunk.TextDelta("答复"),
                new ChatChunk.Finished("stop")))).isEmpty());
    }

    @Test
    void arguments不是JSON对象也判违规() {
        var violation = guard.validate(LlmTurn.merge(List.of(
                new ChatChunk.ToolCallDelta(0, "call_1", "asset_search", "[1,2]"))));
        assertTrue(violation.isPresent());
    }
}
