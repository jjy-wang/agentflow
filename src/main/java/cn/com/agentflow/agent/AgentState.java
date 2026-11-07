package cn.com.agentflow.agent;

import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ToolCall;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * 可序列化到 state_json 的运行状态快照。
 * 崩溃恢复时从数据库读回，用于恢复进度（已完成的步骤不重跑）。
 * 审批挂起时额外保存待执行的工具调用批次，审批通过后据此精确补执行且不重跑已成功步骤。
 */
public record AgentState(String runId,
                         int step,
                         int toolCallCount,
                         int tokenUsed,
                         int promptTokensTotal,
                         long elapsedMs,
                         long eventSeq,
                         List<ChatMessage> messages,
                         List<ToolCall> pendingToolCalls) {

    /** 历史快照可能不含新字段，反序列化后统一归一化为空列表，避免下游 NPE */
    public AgentState {
        messages = messages == null ? List.of() : List.copyOf(messages);
        pendingToolCalls = pendingToolCalls == null ? List.of() : List.copyOf(pendingToolCalls);
    }

    /** 从运行上下文拍快照，消息列表做防御性拷贝 */
    public static AgentState from(RunContext ctx) {
        return new AgentState(
                ctx.getRunId(),
                ctx.getStep(),
                ctx.getToolCallCount(),
                ctx.getTokenUsed(),
                ctx.getPromptTokensTotal(),
                ctx.elapsedMs(),
                ctx.lastEventSeq(),
                List.copyOf(ctx.getMessages()),
                List.copyOf(ctx.getPendingToolCalls()));
    }

    public String toJson(ObjectMapper mapper) {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new AgentException("STATE_SERIALIZE_ERROR", "运行状态序列化失败: " + e.getMessage());
        }
    }

    public static AgentState fromJson(ObjectMapper mapper, String json) {
        try {
            return mapper.readValue(json, AgentState.class);
        } catch (Exception e) {
            throw new AgentException("STATE_DESERIALIZE_ERROR", "运行状态反序列化失败: " + e.getMessage());
        }
    }

    /** 把快照中的历史消息回灌到新的运行上下文（断点续跑用） */
    public void restoreMessagesInto(RunContext ctx) {
        ctx.getMessages().addAll(new ArrayList<>(messages));
    }
}
