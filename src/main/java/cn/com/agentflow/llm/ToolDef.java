package cn.com.agentflow.llm;

import com.fasterxml.jackson.databind.JsonNode;

public record ToolDef(String name, String description, JsonNode parameters) {
}
