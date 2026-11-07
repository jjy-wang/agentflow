package cn.com.agentflow.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * 事件回放：PG（t_run_event）为主，实例重启后回放窗口不丢失；
 * Redis List（最近 500 条 / TTL 30 分钟）为辅，PG 查询失败或无数据时降级。
 * 心跳 id 固定为 0，回放侧忽略 id<=0 的事件。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventReplayStore {

    private static final int MAX_BUFFER = 500;

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;

    /** Redis 短窗口热备：写失败不影响主链路 */
    public void append(String runId, SseEvent event) {
        // 滞后提示是连接级瞬时事件，不进入回放，避免污染热窗口并占用 500 条名额
        if (EventType.LAGGED.equals(event.type())) {
            return;
        }
        try {
            String key = key(runId);
            redis.opsForList().rightPush(key, mapper.writeValueAsString(event));
            redis.opsForList().trim(key, -MAX_BUFFER, -1);
            redis.expire(key, Duration.ofMinutes(30));
        } catch (Exception ignored) {
            // 回放是增强能力，写失败不影响主链路
        }
    }

    public Flux<SseEvent> replay(String runId, String lastEventId) {
        if (lastEventId == null || lastEventId.isBlank()) {
            return Flux.empty();
        }
        long last;
        try {
            last = Long.parseLong(lastEventId);
        } catch (NumberFormatException e) {
            return Flux.empty();
        }
        // PG 为主：查不到（历史 run 或落库降级过）再回退 Redis 短窗口
        return replayFromPg(runId, last)
                .onErrorResume(err -> {
                    log.warn("PG 回放查询失败，降级 Redis: runId={} err={}", runId, err.toString());
                    return replayFromRedis(runId, last);
                })
                .switchIfEmpty(Flux.defer(() -> replayFromRedis(runId, last)));
    }

    /** PG 主回放：按 run_id + seq > lastEventId 单调递增返回 */
    private Flux<SseEvent> replayFromPg(String runId, long last) {
        return Mono.fromCallable(() -> jdbc.query(
                        "SELECT seq, type, data FROM t_run_event WHERE run_id=? AND seq>? ORDER BY seq",
                        (rs, i) -> new SseEvent(rs.getLong("seq"), rs.getString("type"), rs.getString("data")),
                        runId, last))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(Flux::fromIterable);
    }

    /** Redis 辅助回放：短窗口（500 条 / 30 分钟），忽略 id<=0 的心跳 */
    private Flux<SseEvent> replayFromRedis(String runId, long last) {
        return Flux.defer(() -> {
                    List<String> raw = redis.opsForList().range(key(runId), 0, -1);
                    if (raw == null) {
                        return Flux.<SseEvent>empty();
                    }
                    return Flux.fromIterable(raw)
                            .map(this::parse)
                            .filter(Objects::nonNull)
                            // 心跳 id 固定为 0：回放侧忽略 id<=0 的事件，只回放单调递增的业务事件；
                            // 滞后提示同理只是连接内提示，不参与回放
                            .filter(e -> e.id() > 0 && e.id() > last)
                            .filter(e -> !EventType.LAGGED.equals(e.type()));
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private SseEvent parse(String json) {
        try {
            return mapper.readValue(json, SseEvent.class);
        } catch (Exception e) {
            return null;
        }
    }

    private String key(String runId) {
        return "run:" + runId + ":events";
    }
}
