package cn.com.agentflow.agent;

import cn.com.agentflow.llm.ToolCall;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * 结构化输出协议守卫：校验一轮模型输出是否符合输出协议。
 * 协议约定：assistant 输出只能是二选一——
 *   1) 纯文本最终答复（无 tool_calls）；
 *   2) 一个或多个 tool_calls，且每个 function.arguments 必须是合法 JSON 对象。
 * 模型偶发吐出半截 JSON（截断、未闭合括号）时，此处统一拦截，
 * 由 ReActLoop 把"格式错误 + 协议说明"回灌上下文重试，而不是直接判 run 失败。
 */
@Component
public class OutputProtocolGuard {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** 返回违规描述；空 Optional 表示本轮输出合法 */
    public Optional<String> validate(LlmTurn turn) {
        if (!turn.hasToolCalls()) {
            return Optional.empty();
        }
        for (ToolCall call : turn.toolCalls()) {
            if (call.name() == null || call.name().isBlank()) {
                return Optional.of("工具调用缺少 name 字段（callId=" + call.id() + "）");
            }
            String args = call.argumentsJson();
            try {
                JsonNode node = MAPPER.readTree(args == null || args.isBlank() ? "{}" : args);
                if (!node.isObject()) {
                    return Optional.of("工具 " + call.name() + " 的 arguments 不是 JSON 对象: " + abbreviate(args));
                }
            } catch (Exception e) {
                return Optional.of("工具 " + call.name() + " 的 arguments 不是合法 JSON: " + abbreviate(args));
            }
        }
        return Optional.empty();
    }

    /** 回灌给模型的系统反馈：说明错误 + 正确协议，要求按协议重新输出 */
    public String feedback(String violation) {
        return "【系统反馈】你上一轮的输出未通过结构化输出协议校验：" + violation + "。"
                + "输出协议要求：最终答复直接输出纯文本；需要调用工具时必须通过 tool_calls 返回，"
                + "且每个 function.arguments 必须是合法 JSON 对象（示例 {\"keyword\":\"demo\"}），"
                + "不要输出未闭合或截断的 JSON。请严格按协议重新输出。";
    }

    private String abbreviate(String s) {
        if (s == null) {
            return "null";
        }
        return s.length() <= 80 ? s : s.substring(0, 80) + "...";
    }
}
