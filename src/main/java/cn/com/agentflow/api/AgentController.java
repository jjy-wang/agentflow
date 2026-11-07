package cn.com.agentflow.api;

import cn.com.agentflow.api.dto.RunRequest;
import cn.com.agentflow.auth.TenantContext;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.run.RunService;
import cn.com.agentflow.run.RunState;
import cn.com.agentflow.stream.*;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.time.Duration;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/v1/agent")
public class AgentController {

    private final RunService runService;
    private final RunEventBus eventBus;
    private final EventReplayStore replayStore;
    private final SseConnectionRegistry connections;
    private final AgentFlowProperties props;

    /**
     * SSE 兜底超时 = run 总预算 + 30 秒。
     * 必须跟随 agent.max-duration-ms，不能写死：否则预算调大后连接会被提前掐断，
     * 客户端看到截断流、服务端 run 却还在跑。
     */
    private Duration sseHardTimeout() {
        return Duration.ofMillis(props.getAgent().getMaxDurationMs()).plusSeconds(30);
    }

    /** 异步创建 run，立即返回 runId（长任务不占连接） */
    @PostMapping("/runs")
    public Mono<ApiResult<String>> create(@Valid @RequestBody RunRequest req) {
        return TenantContext.current()
                .flatMap(tenantId -> runService.create(tenantId, req)
                        .map(run -> ApiResult.ok(run.runId())));
    }

    /** 流式执行：边跑边推事件 */
    @PostMapping(value = "/runs/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> stream(@Valid @RequestBody RunRequest req,
                                                @RequestHeader(value = "Last-Event-ID", required = false) String lastEventId) {
        return TenantContext.current()
                .flatMapMany(tenantId -> runService.create(tenantId, req)
                        .flatMapMany(run -> {
                            String runId = run.runId();
                            Flux<SseEvent> live = eventBus.subscribe(runId);
                            Flux<SseEvent> replay = replayStore.replay(runId, lastEventId);
                            return Flux.concat(replay, live)
                                    .doOnSubscribe(s -> connections.open(tenantId))
                                    .doOnNext(e -> replayStore.append(runId, e))
                                    .doFinally(sig -> {
                                        connections.close(tenantId);
                                        if (sig != SignalType.CANCEL) {
                                            eventBus.complete(runId);
                                        }
                                    })
                                    // 取消必须级联：否则模型连接、信号量、配额都不释放。
                                    // 必须放在 takeUntil 之后：takeUntil 命中终态事件时会向上游发 cancel，
                                    // 若监听放在上游会把「正常结束」误判成客户端断连，覆盖终态为 CANCELED。
                                    .mergeWith(heartbeat())
                                    .takeUntil(e -> e.type().startsWith("run.") && !e.type().equals("run.started")
                                            && !e.type().equals("run.waiting_approval"))
                                    .doOnCancel(() -> runService.cancel(runId, "CLIENT_DISCONNECTED").subscribe());
                        }))
                .map(e -> ServerSentEvent.builder(e.data())
                        .id(String.valueOf(e.id()))
                        .event(e.type())
                        .build())
                // 兜底超时：run 预算 + 30 秒（跟随配置，不写死）
                .timeout(sseHardTimeout());
    }

    /** 断线后重新订阅（不重新创建 run） */
    @GetMapping(value = "/runs/{runId}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> subscribe(@PathVariable String runId,
                                                   @RequestHeader(value = "Last-Event-ID", required = false) String lastEventId) {
        return TenantContext.current()
                .flatMapMany(tenantId -> runService.assertOwner(tenantId, runId)
                        .thenMany(Flux.concat(replayStore.replay(runId, lastEventId), eventBus.subscribe(runId))))
                .map(e -> ServerSentEvent.builder(e.data())
                        .id(String.valueOf(e.id())).event(e.type()).build())
                .timeout(sseHardTimeout());
    }

    @GetMapping("/runs/{runId}")
    public Mono<ApiResult<RunState>> status(@PathVariable String runId) {
        return TenantContext.current()
                .flatMap(tenantId -> runService.status(tenantId, runId).map(ApiResult::ok));
    }

    @PostMapping("/runs/{runId}/cancel")
    public Mono<ApiResult<Boolean>> cancel(@PathVariable String runId) {
        return TenantContext.current()
                .flatMap(tenantId -> runService.cancelAndConfirm(tenantId, runId).map(ApiResult::ok));
    }

    /** 心跳：id 固定为 0，不参与 Last-Event-ID 单调序列，回放侧忽略 id<=0 的事件 */
    private Flux<SseEvent> heartbeat() {
        return Flux.interval(Duration.ofSeconds(15))
                .map(i -> new SseEvent(0, EventType.PING, "{}"));
    }

    public record ApiResult<T>(int code, String message, T data) {
        public static <T> ApiResult<T> ok(T data) {
            return new ApiResult<>(0, "OK", data);
        }
    }
}
