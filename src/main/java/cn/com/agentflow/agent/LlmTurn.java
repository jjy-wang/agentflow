package cn.com.agentflow.agent;

import cn.com.agentflow.llm.ChatChunk;
import cn.com.agentflow.llm.ToolCall;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** 一轮模型输出的聚合结果：文本 + 工具调用 + 用量 */
public class LlmTurn {

    private final StringBuilder text = new StringBuilder();
    private final Map<Integer, Builder> toolBuilders = new TreeMap<>();
    private int promptTokens;
    private int completionTokens;
    private String finishReason = "stop";

    private static final class Builder {
        String id;
        String name;
        final StringBuilder args = new StringBuilder();
    }

    public static LlmTurn merge(List<ChatChunk> chunks) {
        LlmTurn turn = new LlmTurn();
        for (ChatChunk c : chunks) {
            if (c instanceof ChatChunk.TextDelta t) {
                turn.text.append(t.text());
            } else if (c instanceof ChatChunk.ToolCallDelta d) {
                Builder b = turn.toolBuilders.computeIfAbsent(d.index(), k -> new Builder());
                if (d.callId() != null) {
                    b.id = d.callId();
                }
                if (d.name() != null) {
                    b.name = d.name();
                }
                if (d.argsFragment() != null) {
                    b.args.append(d.argsFragment());
                }
            } else if (c instanceof ChatChunk.Usage u) {
                turn.promptTokens = u.promptTokens();
                turn.completionTokens = u.completionTokens();
            } else if (c instanceof ChatChunk.Finished f) {
                turn.finishReason = f.finishReason();
            }
        }
        return turn;
    }

    public String text() {
        return text.toString();
    }

    public boolean hasToolCalls() {
        return !toolBuilders.isEmpty();
    }

    public List<ToolCall> toolCalls() {
        List<ToolCall> list = new ArrayList<>();
        toolBuilders.forEach((idx, b) -> {
            if (b.name != null) {
                list.add(new ToolCall(
                        b.id != null ? b.id : ("call_" + idx),
                        b.name,
                        b.args.length() == 0 ? "{}" : b.args.toString()));
            }
        });
        return list;
    }

    public int promptTokens() {
        return promptTokens;
    }

    public int completionTokens() {
        return completionTokens;
    }

    public String finishReason() {
        return finishReason;
    }
}
