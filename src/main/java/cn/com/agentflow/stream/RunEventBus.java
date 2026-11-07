package cn.com.agentflow.stream;

import cn.com.agentflow.config.AgentFlowProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 同一 run 的事件广播：多客户端可同时订阅。
 * 发布事件时双写 PG（t_run_event）：实例重启后回放窗口不丢失；
 * 落库失败仅降级记日志，不影响主链路。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RunEventBus {

    /**
     * 共享 sink 的有界缓冲。Reactor 3.6 的 multicast sink 不支持指定溢出策略，
     * 因此慢消费者隔离不依赖它：所有客户端订阅都在 {@link #subscribe(String)} 的每连接缓冲处
     * 向 sink 声明无界需求，共享缓冲不会被慢客户端打满，sink 也就不会因客户端溢出而终止；
     * 保留 512 仅为订阅建立瞬间的突发兜底。
     */
    private static final int SINK_BUFFER = 512;

    private final Map<String, Sinks.Many<SseEvent>> sinks = new ConcurrentHashMap<>();

    private final JdbcTemplate jdbc;
    private final SseConnectionRegistry connections;
    private final AgentFlowProperties props;

    public Sinks.Many<SseEvent> create(String runId) {
        Sinks.Many<SseEvent> live = Sinks.many().multicast().onBackpressureBuffer(SINK_BUFFER, false);
        // 落库走独立 sink：与客户端订阅解耦，客户端断开/被断连都不会影响事件持久化
        Sinks.Many<SseEvent> persistence = Sinks.many().unicast().onBackpressureBuffer();
        persistence.asFlux()
                // concatMap 保证同一 run 内按 seq 顺序写入；心跳 id=0 由 PersistingSink 直接跳过
                .concatMap(e -> Mono.fromRunnable(() -> persist(runId, e))
                        .subscribeOn(Schedulers.boundedElastic()))
                .onErrorContinue((err, e) -> log.warn("事件落库失败，跳过不影响主链路: runId={} event={} err={}",
                        runId, e, err.toString()))
                .subscribe();
        Sinks.Many<SseEvent> sink = new PersistingSink(live, persistence);
        sinks.put(runId, sink);
        return sink;
    }

    /** 双写 PG：(run_id, seq) 唯一约束保证幂等，崩溃续跑重发同 seq 事件时首写胜出 */
    private void persist(String runId, SseEvent e) {
        jdbc.update("INSERT INTO t_run_event(run_id, seq, type, data) VALUES (?,?,?,?) "
                        + "ON CONFLICT (run_id, seq) DO NOTHING",
                runId, e.id(), e.type(), e.data());
    }

    public Flux<SseEvent> subscribe(String runId) {
        Sinks.Many<SseEvent> sink = sinks.get(runId);
        return sink == null ? Flux.empty() : isolateSlowSubscriber(runId, sink.asFlux());
    }

    /**
     * 连接级慢消费者隔离：每个订阅各自一份有界缓冲（agentflow.stream.write-buffer-size），
     * 溢出时显式 DROP_OLDEST——只丢这条连接最旧的未消费事件，优先保住新事件时效，
     * 且丢弃发生在连接缓冲而非共享 sink，一个慢连接不会影响其他连接。
     *
     * <p>为什么丢事件是安全的：事件在发布时已双写 PG（t_run_event），并按
     * Last-Event-ID 支持回放（PG 为主、Redis 短窗口兜底）。丢最旧只是"降级为少推几条"，
     * 客户端收到 stream.lagged 提示后带建议的 lastEventId 重连即可补齐，run 最终状态不丢。
     *
     * <p>单连接连续丢弃超过 agentflow.stream.max-lag-events 时主动断开该连接，
     * 避免长期占用内存与 CPU；客户端重连后走回放链路，属于可恢复的降级。
     */
    private Flux<SseEvent> isolateSlowSubscriber(String runId, Flux<SseEvent> live) {
        return Flux.defer(() -> {
            SseConnectionRegistry.LagGuard guard = connections.newLagGuard();
            AtomicBoolean lagging = new AtomicBoolean(false);
            return live
                    .onBackpressureBuffer(Math.max(1, props.getStream().getWriteBufferSize()),
                            dropped -> {
                                lagging.set(true);
                                guard.onDropped();
                            },
                            BufferOverflowStrategy.DROP_OLDEST)
                    // prefetch=1：一次只向上游多要一条，丢弃判断仍由下游真实消费速度决定；
                    // 正常客户端逐条透传，事件顺序与内容完全不变
                    .concatMap(event -> {
                        if (guard.overThreshold()) {
                            guard.markClosed();
                            log.warn("SSE 慢消费者连续丢弃超过阈值，主动关闭连接等待 Last-Event-ID 重连回放: "
                                            + "runId={} dropped={} threshold={}",
                                    runId, guard.consecutiveDrops(), props.getStream().getMaxLagEvents());
                            return Flux.<SseEvent>error(new SseLagExceededException(runId,
                                    guard.consecutiveDrops(), props.getStream().getMaxLagEvents()));
                        }
                        if (lagging.getAndSet(false)) {
                            // 先补发滞后提示（建议的 lastEventId = 已投递到的事件 id），再发当前事件
                            SseEvent hint = lagHint(guard);
                            guard.recordDeliveredAfterLag(event.id());
                            return Flux.just(hint, event);
                        }
                        guard.recordDelivered(event.id());
                        return Flux.just(event);
                    }, 1);
        });
    }

    /** 滞后提示：id 取已投递到的事件 id，客户端直接用它作为重连时的 Last-Event-ID 即可补齐缺口 */
    private SseEvent lagHint(SseConnectionRegistry.LagGuard guard) {
        long lastEventId = guard.lastDeliveredId();
        return new SseEvent(lastEventId, EventType.LAGGED,
                "{\"lagged\":true,\"lastEventId\":" + lastEventId
                        + ",\"dropped\":" + guard.consecutiveDrops() + "}");
    }

    /**
     * 关闭广播。注意不能顺手结束落库侧：调用方（SSE 连接 doFinally）在慢消费者被断连时也会走到这里，
     * 此时 run 仍在执行，必须继续落库，否则重连后的回放会缺尾部事件。
     * 落库侧由终态事件（run.succeeded/failed/canceled）收尾；从未产生事件的异常退出则一并关闭。
     */
    public void complete(String runId) {
        Sinks.Many<SseEvent> sink = sinks.remove(runId);
        if (sink instanceof PersistingSink persisting) {
            persisting.completeLive();
        } else if (sink != null) {
            sink.tryEmitComplete();
        }
    }

    public boolean exists(String runId) {
        return sinks.containsKey(runId);
    }

    /**
     * 发布入口：先把业务事件投递到独立落库 sink，再广播给在线订阅者。
     * 落库与广播互不阻塞，客户端订阅的背压/断连不会反过来影响持久化与回放链路。
     */
    private static final class PersistingSink implements Sinks.Many<SseEvent> {

        private final Sinks.Many<SseEvent> live;
        private final Sinks.Many<SseEvent> persistence;
        private final AtomicBoolean emitted = new AtomicBoolean(false);

        private PersistingSink(Sinks.Many<SseEvent> live, Sinks.Many<SseEvent> persistence) {
            this.live = live;
            this.persistence = persistence;
        }

        @Override
        public Sinks.EmitResult tryEmitNext(SseEvent event) {
            if (event.id() > 0) {
                emitted.set(true);
                persistence.tryEmitNext(event);
                // 终态事件即 run 的结束边界：落库队列排空后收尾，之后不再有事件
                if (isTerminal(event.type())) {
                    persistence.tryEmitComplete();
                }
            }
            return live.tryEmitNext(event);
        }

        /** 只关闭广播侧，落库侧留给终态事件收尾（慢消费者被断连时 run 仍在执行） */
        private void completeLive() {
            if (!emitted.get()) {
                persistence.tryEmitComplete();
            }
            live.tryEmitComplete();
        }

        private static boolean isTerminal(String type) {
            return EventType.RUN_SUCCEEDED.equals(type)
                    || EventType.RUN_FAILED.equals(type)
                    || EventType.RUN_CANCELED.equals(type);
        }

        @Override
        public Sinks.EmitResult tryEmitComplete() {
            persistence.tryEmitComplete();
            return live.tryEmitComplete();
        }

        @Override
        public Sinks.EmitResult tryEmitError(Throwable error) {
            persistence.tryEmitError(error);
            return live.tryEmitError(error);
        }

        @Override
        public void emitNext(SseEvent event, Sinks.EmitFailureHandler failureHandler) {
            if (event.id() > 0) {
                persistence.emitNext(event, failureHandler);
            }
            live.emitNext(event, failureHandler);
        }

        @Override
        public void emitComplete(Sinks.EmitFailureHandler failureHandler) {
            persistence.emitComplete(failureHandler);
            live.emitComplete(failureHandler);
        }

        @Override
        public void emitError(Throwable error, Sinks.EmitFailureHandler failureHandler) {
            persistence.emitError(error, failureHandler);
            live.emitError(error, failureHandler);
        }

        @Override
        public int currentSubscriberCount() {
            return live.currentSubscriberCount();
        }

        @Override
        public Flux<SseEvent> asFlux() {
            return live.asFlux();
        }

        @Override
        public Object scanUnsafe(reactor.core.Scannable.Attr key) {
            return live.scanUnsafe(key);
        }
    }
}
