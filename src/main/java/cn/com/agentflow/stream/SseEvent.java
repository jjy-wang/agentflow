package cn.com.agentflow.stream;

/** SSE 事件：id 必须单调递增，是 Last-Event-ID 回放的基础 */
public record SseEvent(long id, String type, String data) {
}
