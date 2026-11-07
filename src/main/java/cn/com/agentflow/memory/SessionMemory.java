package cn.com.agentflow.memory;

import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ToolCall;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 会话记忆：t_session 的滚动摘要 + t_message 的最近消息。
 * JDBC 为阻塞调用，统一切到 boundedElastic，不占用事件循环。
 */
@Slf4j
@Service
public class SessionMemory {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public SessionMemory(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /** 最近 N 条消息，按时间正序返回（库里倒序取再反转） */
    public Mono<List<ChatMessage>> loadRecent(long sessionId, int limit) {
        return Mono.fromCallable(() -> {
                    List<Map<String, Object>> rows = jdbc.queryForList(
                            "SELECT role, content, tool_calls, tool_call_id FROM t_message "
                                    + "WHERE session_id = ? ORDER BY id DESC LIMIT ?",
                            sessionId, limit);
                    List<ChatMessage> messages = new ArrayList<>(rows.size());
                    for (int i = rows.size() - 1; i >= 0; i--) {
                        messages.add(toMessage(rows.get(i)));
                    }
                    return messages;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /** 追加一条消息；assistant 的工具调用序列化进 tool_calls JSONB */
    public Mono<Void> append(long sessionId, long tenantId, ChatMessage message) {
        return Mono.fromRunnable(() -> jdbc.update(
                        "INSERT INTO t_message(session_id, tenant_id, role, content, tool_calls, tool_call_id) "
                                + "VALUES (?, ?, ?, ?, ?::jsonb, ?)",
                        sessionId, tenantId, message.role(), message.content(),
                        serializeToolCalls(message), message.toolCallId()))
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /** 读会话摘要；无摘要返回空 Mono */
    public Mono<String> summaryOf(long sessionId) {
        return Mono.fromCallable(() -> {
                    List<String> rows = jdbc.queryForList(
                            "SELECT summary FROM t_session WHERE id = ?", String.class, sessionId);
                    return rows.isEmpty() ? null : rows.get(0);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(s -> s == null || s.isBlank() ? Mono.empty() : Mono.just(s));
    }

    /** 更新摘要与覆盖步数：增量压缩从 summary_upto_step 续 */
    public Mono<Void> updateSummary(long sessionId, String summary, int uptoStep) {
        return Mono.fromRunnable(() -> jdbc.update(
                        "UPDATE t_session SET summary = ?, summary_upto_step = ?, updated_at = now() "
                                + "WHERE id = ?",
                        summary, uptoStep, sessionId))
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    private ChatMessage toMessage(Map<String, Object> row) {
        String role = (String) row.get("role");
        String content = (String) row.get("content");
        return switch (role) {
            case "system" -> ChatMessage.system(content);
            case "user" -> ChatMessage.user(content);
            case "assistant" -> ChatMessage.assistant(content, parseToolCalls(row.get("tool_calls")));
            case "tool" -> ChatMessage.tool((String) row.get("tool_call_id"), content);
            default -> {
                log.warn("未知消息角色按 user 处理: role={}", role);
                yield ChatMessage.user(content);
            }
        };
    }

    /** tool_calls 列可能是 JSONB 串或 null；解析失败不阻断历史加载 */
    private List<ToolCall> parseToolCalls(Object json) {
        if (json == null) {
            return null;
        }
        try {
            return mapper.readValue(json.toString(), new TypeReference<>() {
            });
        } catch (Exception e) {
            log.warn("tool_calls 反序列化失败，按无工具调用处理: {}", e.toString());
            return null;
        }
    }

    private String serializeToolCalls(ChatMessage message) {
        if (message.toolCalls() == null || message.toolCalls().isEmpty()) {
            return null;
        }
        try {
            return mapper.writeValueAsString(message.toolCalls());
        } catch (Exception e) {
            // 序列化失败不应丢整条消息：退化为只存文本
            log.warn("tool_calls 序列化失败，只存消息文本: {}", e.toString());
            return null;
        }
    }
}
