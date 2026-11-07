package cn.com.agentflow.llm;

/** 模型发起的一次工具调用。argumentsJson 是模型生成的 JSON 字符串，必须经过 schema 校验。 */
public record ToolCall(String id, String name, String argumentsJson) {
}
