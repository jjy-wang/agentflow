package cn.com.agentflow.llm;

import java.util.List;

public record ChatRequest(String model,
                          List<ChatMessage> messages,
                          List<ToolDef> tools,
                          int maxTokens,
                          double temperature) {
}
