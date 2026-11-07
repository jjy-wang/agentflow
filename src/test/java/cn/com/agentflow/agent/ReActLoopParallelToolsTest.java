package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ChatChunk;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.LlmInvoker;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.memory.MemoryRetrievalService;
import cn.com.agentflow.stream.SseEvent;
import cn.com.agentflow.telemetry.GenAiTelemetry;
import cn.com.agentflow.tool.ToolExecutor;
import cn.com.agentflow.tool.ToolRegistry;
import cn.com.agentflow.tool.ToolResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 同轮工具调用有界并行：TOOL_CALL 原始顺序、TOOL_RESULT 完成顺序、
 * 上下文按原始顺序组装、单点失败降级、全部失败抛错、并发上限生效。
 */
class ReActLoopParallelToolsTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();

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
        RunContext ctx = new RunContext("1", "run-1", 100L, "REACT", "mock-large",
                new RunLimits(12, 20, 60_000, 32_000), sink);
        ctx.append(ChatMessage.user("请并行查询"));
        return ctx;
    }

    /** 首轮返回 N 个工具调用，次轮返回最终答案 */
    private void stubLlm(int toolCount) {
        when(llmInvoker.invoke(any()))
                .thenReturn(toolCallTurn(toolCount))
                .thenReturn(Flux.just(new ChatChunk.TextDelta("完成"),
                        new ChatChunk.Usage(5, 2), new ChatChunk.Finished("stop")));
    }

    private Flux<ChatChunk> toolCallTurn(int toolCount) {
        List<ChatChunk> chunks = new java.util.ArrayList<>();
        for (int i = 0; i < toolCount; i++) {
            chunks.add(new ChatChunk.ToolCallDelta(i, "call_" + i, "fake_tool", "{\"i\":" + i + "}"));
        }
        chunks.add(new ChatChunk.Usage(10, 5));
        chunks.add(new ChatChunk.Finished("tool_calls"));
        return Flux.fromIterable(chunks);
    }

    /** 可控延迟的假执行器：按 callId 后缀决定耗时，可指定哪些调用失败 */
    private void stubExecutor(int baseDelayMs, Predicate<ToolCall> fail) {
        when(toolExecutor.execute(any(), any())).thenAnswer(invocation -> {
            ToolCall call = invocation.getArgument(0);
            int index = Integer.parseInt(call.id().substring("call_".length()));
            return Mono.defer(() -> {
                maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                Mono<ToolResult> result = fail.test(call)
                        ? Mono.error(new IllegalStateException("boom-" + call.id()))
                        : Mono.just(ToolResult.ok(mapper.createObjectNode().put("call", call.id())));
                return Mono.delay(Duration.ofMillis(baseDelayMs / (index + 1)))
                        .then(result)
                        // 在向 downstream 发信号前减计数，避免被测框架订阅下一个内层 Mono 时误判在途数
                        .doOnSuccess(ignored -> inFlight.decrementAndGet())
                        .doOnError(error -> inFlight.decrementAndGet());
            });
        });
    }

    private List<String> callIds(String type) {
        return events.stream().filter(e -> type.equals(e.type()))
                .map(e -> e.data())
                .map(data -> {
                    int start = data.indexOf("call_");
                    int end = data.indexOf('"', start);
                    return data.substring(start, end);
                })
                .collect(Collectors.toList());
    }

    @Test
    void 结果事件按完成顺序_上下文按原始顺序组装() {
        stubLlm(3);
        // 延迟随下标递减：call_2 先完成，call_0 最后完成
        stubExecutor(300, call -> false);

        RunContext ctx = newContext();
        StepVerifier.create(newLoop().run(ctx, "sys"))
                .expectNext("完成")
                .verifyComplete();

        assertEquals(List.of("call_0", "call_1", "call_2"), callIds("tool.call"),
                "TOOL_CALL 必须按模型给出的原始顺序先全部发出");
        assertEquals(List.of("call_2", "call_1", "call_0"), callIds("tool.result"),
                "TOOL_RESULT 必须按完成顺序发出");

        List<String> toolMessageIds = ctx.getMessages().stream()
                .filter(m -> "tool".equals(m.role()))
                .map(m -> m.toolCallId())
                .collect(Collectors.toList());
        assertEquals(List.of("call_0", "call_1", "call_2"), toolMessageIds,
                "回灌上下文必须按原始调用顺序组装，保证模型视角可复现");
    }

    @Test
    void 单个工具失败降级为失败结果_整轮继续() {
        stubLlm(3);
        stubExecutor(60, call -> "call_1".equals(call.id()));

        RunContext ctx = newContext();
        StepVerifier.create(newLoop().run(ctx, "sys"))
                .expectNext("完成")
                .verifyComplete();

        List<ChatMessage> toolMessages = ctx.getMessages().stream()
                .filter(m -> "tool".equals(m.role()))
                .toList();
        assertEquals(3, toolMessages.size(), "失败调用也必须产生一条 tool 结果回灌给模型");
        assertTrue(toolMessages.get(1).content().contains("boom-call_1"),
                "失败结果必须带错误文本，模型据此自我纠正");
    }

    @Test
    void 全部工具失败才向上抛错() {
        stubLlm(2);
        stubExecutor(20, call -> true);

        RunContext ctx = newContext();
        StepVerifier.create(newLoop().run(ctx, "sys"))
                .expectErrorMatches(e -> e instanceof AgentException ae
                        && "TOOL_ALL_FAILED".equals(ae.getCode()))
                .verify();

        assertEquals(2, callIds("tool.result").size(), "每个失败调用仍应先发结果事件");
    }

    @Test
    void 并发上限生效且确实并行() {
        props.getTool().getParallel().setMaxConcurrency(2);
        stubLlm(5);
        stubExecutor(120, call -> false);

        StepVerifier.create(newLoop().run(newContext(), "sys"))
                .expectNext("完成")
                .verifyComplete();

        assertEquals(2, maxInFlight.get(), "在途工具数必须被 maxConcurrency 严格限制");
    }

    @Test
    void 关闭并行开关退回串行() {
        props.getTool().getParallel().setEnabled(false);
        stubLlm(4);
        stubExecutor(40, call -> false);

        StepVerifier.create(newLoop().run(newContext(), "sys"))
                .expectNext("完成")
                .verifyComplete();

        assertEquals(1, maxInFlight.get(), "关闭开关后必须退回一次只跑一个工具");
    }
}
