package cn.com.agentflow.tool;

import com.fasterxml.jackson.databind.JsonNode;

/** 注册后的工具元数据 */
public record ToolMeta(String name,
                       String description,
                       JsonNode schema,
                       int riskLevel,
                       int timeoutMs,
                       ToolHandler handler) {
}
