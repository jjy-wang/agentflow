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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * run / step 两级超时预算：
 * run 级到点即 RUN_TIMEOUT；单步超时按可重试失败处理，最多重试 1 次；
 * 连续两次单步超时才终结 run；超时取消上游必定触发资源回收回调。
 */
class RunTimeoutTest {

    private LlmInvoker llmInvoker;
    private AgentFlowProperties props;
    private ReActLoop loop;
    private Sinks.Many<SseEvent> sink;
    private List<SseEvent> events;

    @BeforeEach
    void setUp() {
        llmInvoker = mock(LlmInvoker.class);
        ToolRegistry toolRegistry = mock(ToolRegistry.class);
        ToolExecutor toolExecutor = mock(ToolExecutor.class);
        ContextBuilder contextBuilder = mock(ContextBuilder.class);
        ContextCompressor contextCompressor = mock(ContextCompressor.class);
        MemoryRetrievalService memoryRetrievalService = mock(MemoryRetrievalService.class);
        props = new AgentFlowProperties();

        when(toolRegistry.definitions(any())).thenReturn(List.of());
        when(contextBuilder.build(any(), any(), any()))
                .thenReturn(new ChatRequest("mock-large", List.of(), List.of(), 256, 0.3));
        when(contextCompressor.maybeCompress(any())).thenReturn(Mono.empty());
        when(memoryRetrievalService.recallSection(any(), any())).thenReturn(Mono.just(""));

        loop = new ReActLoop(llmInvoker, toolRegistry, toolExecutor, contextBuilder,
                contextCompressor, new LoopGuard(), new OutputProtocolGuard(),
                props, memoryRetrievalService, GenAiTelemetry.noop());

        Sinks.Many<SseEvent> eventSink = Sinks.many().multicast().onBackpressureBuffer();
        events = new CopyOnWriteArrayList<>();
        eventSink.asFlux().subscribe(events::add);
        this.sink = eventSink;
    }

    private RunContext newContext() {
        return new RunContext("1", "run-timeout", 100L, "REACT", "mock-large",
                new RunLimits(12, 8, 60_000, 32_000), sink);
    }

    private Flux<ChatChunk> textTurn(String text) {
        return Flux.just(
                new ChatChunk.TextDelta(text),
                new ChatChunk.Usage(8, 4),
                new ChatChunk.Finished("stop"));
    }

    @Test
    void run总预算耗尽_RUN_TIMEOUT且上游资源被释放() {
        props.getAgent().setMaxDurationMs(400);
        props.getAgent().setStepTimeoutMs(300);

        ReActLoop reActLoop = mock(ReActLoop.class);
        AtomicBoolean released = new AtomicBoolean(false);
        when(reActLoop.run(any(), any())).thenReturn(Mono.<String>never()
                .doFinally(signal -> released.set(true)));
        AgentRuntime runtime = new AgentRuntime(reActLoop, mock(PlanExecuteLoop.class), props);

        RunContext ctx = newContext();
        StepVerifier.create(runtime.execute(ctx))
                .expectErrorMatches(e -> e instanceof AgentException ae
                        && "RUN_TIMEOUT".equals(ae.getCode())
                        && ae.getMessage().contains("400ms")
                        && ae.getMessage().contains("已用"))
                .verify(Duration.ofSeconds(3));

        assertTrue(released.get(), "run 超时必须取消上游，触发资源回收回调");
    }

    @Test
    void 单步超时重试一次后成功_run继续() {
        props.getAgent().setMaxDurationMs(5000);
        props.getAgent().setStepTimeoutMs(60);
        when(llmInvoker.invoke(any()))
                .thenReturn(Flux.<ChatChunk>never())
                .thenReturn(textTurn("恢复"));

        RunContext ctx = newContext();
        StepVerifier.create(loop.run(ctx, "sys"))
                .expectNext("恢复")
                .verifyComplete();

        assertEquals(2, ctx.getStep(), "超时重试必须走 loop() 入口并占用步数预算");
        assertTrue(events.stream().anyMatch(e -> "step.started".equals(e.type())
                        && e.data().contains("STEP_TIMEOUT_RETRY")),
                "必须发出 STEP_TIMEOUT_RETRY 事件");
    }

    @Test
    void 连续两次单步超时_STEP_TIMEOUT终结run并释放资源() {
        props.getAgent().setMaxDurationMs(5000);
        props.getAgent().setStepTimeoutMs(50);
        AtomicBoolean released = new AtomicBoolean(false);
        when(llmInvoker.invoke(any())).thenReturn(Flux.<ChatChunk>never()
                .doFinally(signal -> released.set(true)));

        RunContext ctx = newContext();
        StepVerifier.create(loop.run(ctx, "sys"))
                .expectErrorMatches(e -> e instanceof AgentException ae
                        && "STEP_TIMEOUT".equals(ae.getCode())
                        && ae.getMessage().contains("50ms"))
                .verify(Duration.ofSeconds(3));

        assertEquals(2, ctx.getStep(), "重试也计入步数，连续两次超时后 run 失败");
        assertTrue(released.get(), "单步超时取消上游后必须触发资源回收回调");
    }

    @Test
    void 单步超时但总预算已耗尽_RUN_TIMEOUT() {
        props.getAgent().setMaxDurationMs(40);
        props.getAgent().setStepTimeoutMs(25);
        when(llmInvoker.invoke(any())).thenReturn(Flux.<ChatChunk>never());

        RunContext ctx = newContext();
        StepVerifier.create(loop.run(ctx, "sys"))
                .expectErrorMatches(e -> e instanceof AgentException ae
                        && "RUN_TIMEOUT".equals(ae.getCode())
                        && ae.getMessage().contains("40ms"))
                .verify(Duration.ofSeconds(3));
    }
}
