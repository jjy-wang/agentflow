package cn.com.agentflow.memory;

import cn.com.agentflow.agent.LlmTurn;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.LlmInvoker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 对话摘要器：把多轮历史压缩成一段事实性摘要，用于上下文压缩与 session summary。
 * 提示词读 prompts/summarize_zh.txt；读取失败用内置兜底，保证功能永远可用。
 */
@Slf4j
@Component
public class Summarizer {

    /** 兜底提示词：与 prompts/summarize_zh.txt 要求保持一致的最小版本 */
    private static final String FALLBACK_PROMPT =
            "你是对话摘要助手。请把给定的多轮对话历史压缩为一段简洁的事实性摘要。"
                    + "保留用户的最终目标与约束、已完成的工具调用名称及关键结果、未完成的子任务与下一步计划；"
                    + "丢弃寒暄与无信息量的细节。只输出摘要本身，使用简体中文，控制在 500 字以内。";

    private final LlmInvoker llmInvoker;
    private final AgentFlowProperties props;
    private final String promptTemplate;

    public Summarizer(LlmInvoker llmInvoker, AgentFlowProperties props) {
        this.llmInvoker = llmInvoker;
        this.props = props;
        this.promptTemplate = loadPrompt();
    }

    /** 摘要压缩：失败时由调用方决定降级策略，这里不吞错误 */
    public Mono<String> summarize(List<ChatMessage> history) {
        List<ChatMessage> messages = new ArrayList<>(2);
        messages.add(ChatMessage.system(promptTemplate));
        messages.add(ChatMessage.user(render(history)));
        ChatRequest request = new ChatRequest(props.getLlm().getDefaultModel(), messages, null,
                props.getAgent().getSummaryMaxTokens(), 0.2);
        return llmInvoker.invoke(request)
                .collectList()
                .map(LlmTurn::merge)
                .map(LlmTurn::text);
    }

    /** 把历史渲染成纯文本：assistant 的工具调用只留名称与参数，避免刷屏 */
    private String render(List<ChatMessage> history) {
        StringBuilder sb = new StringBuilder();
        for (ChatMessage m : history) {
            sb.append(m.role()).append(": ");
            if (m.content() != null) {
                sb.append(m.content());
            }
            if (m.toolCalls() != null) {
                m.toolCalls().forEach(tc -> sb.append(" [调用工具 ").append(tc.name())
                        .append(' ').append(tc.argumentsJson()).append(']'));
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private String loadPrompt() {
        try {
            return new String(new ClassPathResource("prompts/summarize_zh.txt").getInputStream()
                    .readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("摘要提示词加载失败，使用内置兜底: {}", e.toString());
            return FALLBACK_PROMPT;
        }
    }
}
