package cn.com.agentflow.stream;

import cn.com.agentflow.config.AgentFlowProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Subscription;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * RunEventBus：发布事件双写 PG（t_run_event），seq 单调递增，落库失败不影响主链路；
 * 慢消费者按连接隔离：有界缓冲 DROP_OLDEST + 滞后提示 + 超阈值断连。
 */
@ExtendWith(MockitoExtension.class)
class RunEventBusTest {

    @Mock
    private JdbcTemplate jdbc;

    private final AgentFlowProperties props = new AgentFlowProperties();
    private SimpleMeterRegistry meters;
    private SseConnectionRegistry connections;
    private RunEventBus bus;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        connections = new SseConnectionRegistry(meters, props);
        bus = new RunEventBus(jdbc, connections, props);
    }

    @Test
    void 发布事件时按seq顺序双写PG() {
        Sinks.Many<SseEvent> sink = bus.create("r1");
        sink.tryEmitNext(new SseEvent(1, "run.started", "{}"));
        sink.tryEmitNext(new SseEvent(2, "llm.delta", "{\"delta\":\"a\"}"));
        sink.tryEmitNext(new SseEvent(3, "run.succeeded", "{}"));

        // 落库在 boundedElastic 异步执行：按 seq 单调递增逐条写入
        verify(jdbc, timeout(2000)).update(contains("t_run_event"), eq("r1"), eq(1L), eq("run.started"), eq("{}"));
        verify(jdbc, timeout(2000)).update(contains("t_run_event"), eq("r1"), eq(2L), eq("llm.delta"), eq("{\"delta\":\"a\"}"));
        verify(jdbc, timeout(2000)).update(contains("t_run_event"), eq("r1"), eq(3L), eq("run.succeeded"), eq("{}"));
    }

    @Test
    void 心跳事件id为0不落库() {
        Sinks.Many<SseEvent> sink = bus.create("r1");
        sink.tryEmitNext(new SseEvent(0, "ping", "{}"));

        verify(jdbc, after(300).never()).update(anyString(), any(), any(), any(), any());
    }

    @Test
    void PG写失败时仅降级记日志不影响主链路() {
        when(jdbc.update(anyString(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("pg down"));
        Sinks.Many<SseEvent> sink = bus.create("r1");

        // 主链路（订阅方）照常收到事件；后续事件也不因前一条落库失败而中断
        StepVerifier.create(bus.subscribe("r1"))
                .then(() -> sink.tryEmitNext(new SseEvent(1, "run.started", "{}")))
                .assertNext(e -> assertEquals(1, e.id()))
                .then(() -> sink.tryEmitNext(new SseEvent(2, "run.succeeded", "{}")))
                .assertNext(e -> assertEquals(2, e.id()))
                .thenCancel()
                .verify();
        verify(jdbc, timeout(2000).times(2)).update(anyString(), any(), any(), any(), any());
    }

    @Test
    void 慢订阅者打满缓冲后sink不终止且新事件仍可发送() {
        props.getStream().setWriteBufferSize(4);
        Sinks.Many<SseEvent> sink = bus.create("r1");
        RecordingSubscriber conn = new RecordingSubscriber(0);
        bus.subscribe("r1").subscribe(conn);

        // 客户端一条不读：连接缓冲被打满，共享 sink 与发送方不受影响
        for (long i = 1; i <= 10; i++) {
            assertEquals(Sinks.EmitResult.OK, sink.tryEmitNext(new SseEvent(i, "llm.delta", "{}")));
        }
        assertEquals(Sinks.EmitResult.OK, sink.tryEmitNext(new SseEvent(11, "run.succeeded", "{}")));

        conn.requestMore(20);
        assertEquals(Sinks.EmitResult.OK, sink.tryEmitNext(new SseEvent(12, "run.succeeded", "{}")));

        List<SseEvent> items = conn.items();
        assertFalse(items.isEmpty());
        // 丢弃只发生在这条连接，sink 仍可继续发新事件，且最新事件能送达
        assertEquals(12, lastBusinessId(items));
        // 丢弃后补发的滞后提示存在
        assertTrue(items.stream().anyMatch(e -> EventType.LAGGED.equals(e.type())));
        // 业务事件顺序不乱，提示事件不进入业务序列
        assertTrue(strictlyIncreasingBusinessIds(items));
    }

    @Test
    void 发生丢弃时补发滞后提示事件并建议lastEventId() {
        props.getStream().setWriteBufferSize(2);
        Sinks.Many<SseEvent> sink = bus.create("r1");
        RecordingSubscriber conn = new RecordingSubscriber(1);
        bus.subscribe("r1").subscribe(conn);

        sink.tryEmitNext(new SseEvent(1, "run.started", "{}"));
        assertEquals(1, conn.items().size());
        // 客户端消费到 id=1 后停止读取，2~8 只留最新 2 条，其余丢最旧
        for (long i = 2; i <= 8; i++) {
            sink.tryEmitNext(new SseEvent(i, "llm.delta", "{}"));
        }
        conn.requestMore(20);

        List<SseEvent> items = conn.items();
        int hintIndex = -1;
        for (int i = 0; i < items.size(); i++) {
            if (EventType.LAGGED.equals(items.get(i).type())) {
                hintIndex = i;
                break;
            }
        }
        assertTrue(hintIndex > 0);
        SseEvent hint = items.get(hintIndex);
        SseEvent previous = items.get(hintIndex - 1);
        assertTrue(hint.data().contains("\"lagged\":true"));
        // 提示事件的 id 与 payload 中的 lastEventId 都指向已成功消费到的事件，可直接用于重连回放
        assertEquals(previous.id(), hint.id());
        assertTrue(hint.data().contains("\"lastEventId\":" + previous.id()));
    }

    @Test
    void 连续丢弃超过阈值时主动关闭连接() {
        props.getStream().setWriteBufferSize(2);
        props.getStream().setMaxLagEvents(2);
        Sinks.Many<SseEvent> sink = bus.create("r1");
        RecordingSubscriber conn = new RecordingSubscriber(1);
        bus.subscribe("r1").subscribe(conn);

        sink.tryEmitNext(new SseEvent(1, "run.started", "{}"));
        for (long i = 2; i <= 8; i++) {
            sink.tryEmitNext(new SseEvent(i, "llm.delta", "{}"));
        }
        conn.requestMore(20);

        // 连续丢弃超过阈值：不再补提示，直接断开，客户端带 Last-Event-ID 重连回放
        assertFalse(conn.errors().isEmpty());
        assertTrue(conn.errors().get(0) instanceof SseLagExceededException);
        assertTrue(conn.items().stream().noneMatch(e -> EventType.LAGGED.equals(e.type())));
        assertEquals(1.0, counter("sse.connections.lagged_closed.total"));
        assertTrue(counter("sse.events.dropped.total") > props.getStream().getMaxLagEvents());
    }

    @Test
    void 正常消费速度不产生丢弃也不补发滞后提示() {
        props.getStream().setWriteBufferSize(8);
        Sinks.Many<SseEvent> sink = bus.create("r1");
        List<SseEvent> received = new ArrayList<>();

        StepVerifier.create(bus.subscribe("r1").doOnNext(received::add))
                .then(() -> {
                    for (long i = 1; i <= 20; i++) {
                        sink.tryEmitNext(new SseEvent(i, "llm.delta", "{}"));
                    }
                })
                .expectNextCount(20)
                .thenCancel()
                .verify();

        assertEquals(20, received.size());
        for (int i = 0; i < received.size(); i++) {
            assertEquals(i + 1, received.get(i).id());
            assertFalse(EventType.LAGGED.equals(received.get(i).type()));
        }
        assertEquals(0.0, counter("sse.events.dropped.total"));
    }

    private long lastBusinessId(List<SseEvent> items) {
        for (int i = items.size() - 1; i >= 0; i--) {
            if (!EventType.LAGGED.equals(items.get(i).type())) {
                return items.get(i).id();
            }
        }
        return -1;
    }

    private boolean strictlyIncreasingBusinessIds(List<SseEvent> items) {
        long previous = -1;
        for (SseEvent e : items) {
            if (EventType.LAGGED.equals(e.type())) {
                continue;
            }
            if (e.id() <= previous) {
                return false;
            }
            previous = e.id();
        }
        return true;
    }

    private double counter(String name) {
        Counter c = meters.find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    /** 直接控制 request 数量的订阅者，用于模拟慢客户端 */
    private static final class RecordingSubscriber extends BaseSubscriber<SseEvent> {

        private final List<SseEvent> items = new ArrayList<>();
        private final List<Throwable> errors = new ArrayList<>();
        private final long initialRequest;

        private RecordingSubscriber(long initialRequest) {
            this.initialRequest = initialRequest;
        }

        @Override
        protected void hookOnSubscribe(Subscription s) {
            s.request(initialRequest);
        }

        @Override
        protected void hookOnNext(SseEvent value) {
            items.add(value);
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            errors.add(throwable);
        }

        private void requestMore(long n) {
            upstream().request(n);
        }

        private List<SseEvent> items() {
            return items;
        }

        private List<Throwable> errors() {
            return errors;
        }
    }
}

