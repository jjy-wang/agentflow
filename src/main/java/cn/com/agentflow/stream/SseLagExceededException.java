package cn.com.agentflow.stream;

/**
 * 慢消费者连续丢弃超过阈值：主动断开当前 SSE 连接，
 * 客户端应带 Last-Event-ID 重连并回放，属于可恢复的降级而不是数据丢失。
 */
public class SseLagExceededException extends RuntimeException {

    public SseLagExceededException(String runId, int dropped, int threshold) {
        super("SSE 慢消费者连续丢弃超过阈值: runId=" + runId
                + " dropped=" + dropped + " threshold=" + threshold);
    }
}
