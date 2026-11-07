package cn.com.agentflow.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * 工具调用幂等存储：JDBC 阻塞操作统一切到 boundedElastic
 */
@RequiredArgsConstructor
@Component
public class ToolInvocationStore {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    /** 唯一约束保证同一 (runId, toolCallId) 只会有一个实例插入成功 */
    public Mono<Boolean> tryStart(String runId, String callId, String toolName) {
        return Mono.fromCallable(() -> jdbc.update(
                        "INSERT INTO t_tool_invocation(run_id, tool_call_id, tool_name, status) "
                                + "VALUES (?,?,?,'RUNNING') ON CONFLICT (run_id, tool_call_id) DO NOTHING",
                        runId, callId, toolName) > 0)
                .subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<Void> finish(String runId, String callId, ToolResult result) {
        return Mono.fromRunnable(() -> {
            try {
                jdbc.update("UPDATE t_tool_invocation SET status=?, result_json=?::jsonb "
                                + "WHERE run_id=? AND tool_call_id=?",
                        result.success() ? "SUCCEEDED" : "FAILED",
                        mapper.writeValueAsString(result), runId, callId);
            } catch (Exception e) {
                throw new IllegalStateException("写入工具调用结果失败", e);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    public Mono<String> findResult(String runId, String callId) {
        return Mono.fromCallable(() -> jdbc.queryForObject(
                        "SELECT result_json::text FROM t_tool_invocation WHERE run_id=? AND tool_call_id=?",
                        String.class, runId, callId))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
