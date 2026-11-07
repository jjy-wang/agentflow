package cn.com.agentflow.llm;

public interface ChatChunk {

    /** 文本增量 */
    record TextDelta(String text) implements ChatChunk {
    }

    /** 工具调用分片：同一 index 的分片必须按顺序拼接 */
    record ToolCallDelta(int index, String callId, String name, String argsFragment) implements ChatChunk {
    }

    /** 用量（部分供应商只在最后一个包或单独包给出） */
    record Usage(int promptTokens, int completionTokens) implements ChatChunk {
    }

    /** 结束标记 */
    record Finished(String finishReason) implements ChatChunk {
    }
}
