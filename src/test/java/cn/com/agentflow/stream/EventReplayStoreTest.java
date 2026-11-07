package cn.com.agentflow.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** EventReplayStore：PG 为主、Redis 为辅的断线重连回放，id<=0 的心跳事件被忽略（md 18.1） */
@ExtendWith(MockitoExtension.class)
class EventReplayStoreTest {

    @Mock
    private StringRedisTemplate redis;
    @Mock
    private ListOperations<String, String> listOps;
    @Mock
    private JdbcTemplate jdbc;

    private final ObjectMapper mapper = new ObjectMapper();
    private EventReplayStore store;

    @BeforeEach
    void setUp() {
        lenient().when(redis.opsForList()).thenReturn(listOps);
        // 默认 PG 无数据，既有用例走 Redis 辅助回放
        lenient().when(jdbc.query(anyString(), any(RowMapper.class), anyString(), anyLong()))
                .thenReturn(List.of());
        store = new EventReplayStore(redis, mapper, jdbc);
    }

    private String json(long id, String type) throws Exception {
        return mapper.writeValueAsString(new SseEvent(id, type, "{}"));
    }

    @Test
    void lastEventId为5时只回放id大于5的业务事件() throws Exception {
        when(listOps.range("run:r1:events", 0, -1)).thenReturn(List.of(
                json(3, "run.step"), json(0, "heartbeat"), json(5, "run.step"),
                json(6, "run.step"), json(7, "run.completed")));
        StepVerifier.create(store.replay("r1", "5"))
                .assertNext(e -> assertEquals(6, e.id()))
                .assertNext(e -> assertEquals(7, e.id()))
                .verifyComplete();
    }

    @Test
    void 心跳事件id为0回放时被忽略() throws Exception {
        when(listOps.range("run:r1:events", 0, -1)).thenReturn(List.of(
                json(0, "heartbeat"), json(0, "heartbeat"), json(2, "run.step")));
        StepVerifier.create(store.replay("r1", "1"))
                .assertNext(e -> assertEquals(2, e.id()))
                .verifyComplete();
    }

    @Test
    void lastEventId为null或空白时不回放() {
        StepVerifier.create(store.replay("r1", null)).verifyComplete();
        StepVerifier.create(store.replay("r1", " ")).verifyComplete();
        verify(listOps, never()).range(anyString(), eq(0L), eq(-1L));
    }

    @Test
    void lastEventId非数字时不回放() {
        StepVerifier.create(store.replay("r1", "abc")).verifyComplete();
        verify(listOps, never()).range(anyString(), eq(0L), eq(-1L));
    }

    @Test
    void append写入后立即修剪到最近500条并续期() {
        store.append("r1", new SseEvent(9, "run.step", "{}"));
        verify(listOps).rightPush(eq("run:r1:events"), anyString());
        verify(listOps).trim("run:r1:events", -500, -1);
        verify(redis).expire("run:r1:events", Duration.ofMinutes(30));
    }

    @Test
    void redis写失败时append静默吞掉不影响主链路() {
        when(listOps.rightPush(anyString(), anyString())).thenThrow(new RuntimeException("redis down"));
        // 不抛异常即通过；且 expire 也不应触达（异常已中断流程）
        store.append("r1", new SseEvent(1, "run.step", "{}"));
        verify(redis, never()).expire(anyString(), any(Duration.class));
    }

    @Test
    void PG有数据时优先回放PG且不查Redis() {
        when(jdbc.query(anyString(), any(RowMapper.class), eq("r1"), eq(5L)))
                .thenReturn(List.of(new SseEvent(6, "run.step", "{}"),
                        new SseEvent(7, "run.completed", "{}")));
        StepVerifier.create(store.replay("r1", "5"))
                .assertNext(e -> assertEquals(6, e.id()))
                .assertNext(e -> assertEquals(7, e.id()))
                .verifyComplete();
        verify(listOps, never()).range(anyString(), eq(0L), eq(-1L));
    }

    @Test
    void PG回放结果按seq单调递增连续() {
        when(jdbc.query(anyString(), any(RowMapper.class), eq("r1"), eq(0L)))
                .thenReturn(List.of(new SseEvent(1, "run.started", "{}"),
                        new SseEvent(2, "run.step", "{}"),
                        new SseEvent(3, "run.step", "{}"),
                        new SseEvent(4, "run.completed", "{}")));
        StepVerifier.create(store.replay("r1", "0"))
                .assertNext(e -> assertEquals(1, e.id()))
                .assertNext(e -> assertEquals(2, e.id()))
                .assertNext(e -> assertEquals(3, e.id()))
                .assertNext(e -> assertEquals(4, e.id()))
                .verifyComplete();
    }

    @Test
    void PG查询异常时降级Redis回放() throws Exception {
        when(jdbc.query(anyString(), any(RowMapper.class), anyString(), anyLong()))
                .thenThrow(new RuntimeException("pg down"));
        when(listOps.range("run:r1:events", 0, -1)).thenReturn(List.of(
                json(2, "run.step"), json(3, "run.completed")));
        StepVerifier.create(store.replay("r1", "1"))
                .assertNext(e -> assertEquals(2, e.id()))
                .assertNext(e -> assertEquals(3, e.id()))
                .verifyComplete();
    }
}
