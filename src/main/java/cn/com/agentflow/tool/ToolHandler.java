package cn.com.agentflow.tool;

import cn.com.agentflow.agent.RunContext;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * 工具执行句柄：由 ToolRegistry 反射包装生成。
 * 带上 RunContext 是为了让需要租户/会话上下文的工具（如长期记忆）安全取值，
 * 租户不接受模型传参，避免伪造。
 */
public interface ToolHandler {
    ToolResult execute(JsonNode args, RunContext ctx) throws Exception;
}
