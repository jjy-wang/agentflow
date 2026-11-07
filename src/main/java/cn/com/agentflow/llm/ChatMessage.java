package cn.com.agentflow.llm;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public record ChatMessage(String role, String content, List<ToolCall> toolCalls, String toolCallId) {

    public static ChatMessage system(String content) {
        return new ChatMessage("system", content, null, null);
    }

    public static ChatMessage user(String content) {
        return new ChatMessage("user", content, null, null);
    }

    public static ChatMessage assistant(String content, List<ToolCall> toolCalls) {
        return new ChatMessage("assistant", content, toolCalls, null);
    }

    /** 工具执行结果回灌，必须带 toolCallId，模型才能对上号 */
    public static ChatMessage tool(String toolCallId, String content) {
        return new ChatMessage("tool", content, null, toolCallId);
    }

    /**
     * 仅内部判定用，不是消息字段。
     * 必须 @JsonIgnore：否则会被序列化进 state_json，回读时 Jackson 找不到对应构造参数而报
     * Unrecognized field "assistantWithTools"，导致审批续跑时状态恢复失败。
     */
    @JsonIgnore
    public boolean isAssistantWithTools() {
        return toolCalls != null && !toolCalls.isEmpty();
    }
}
