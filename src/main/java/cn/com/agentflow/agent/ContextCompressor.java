package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.LlmInvoker;
import cn.com.agentflow.util.TokenEstimator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 上下文压缩：历史 token 占用超过预算阈值（compressor-trigger-ratio）时，
 * 调用摘要模型把早期历史压缩成一条摘要消息，最近一轮原样保留，
 * 保证工具调用配对不被截断。
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class ContextCompressor {

    private static final String FALLBACK_SUMMARIZE_PROMPT =
            "你是对话摘要助手。请把给定的对话历史压缩为一段简洁的事实性摘要，"
                    + "保留用户目标、已完成的工具调用及其关键结果、当前未完成的子任务。"
                    + "只输出摘要本身，不要输出其他内容。";

    private final AgentFlowProperties props;
    private final LlmInvoker llmInvoker;

    /** 超阈值则压缩历史，否则直接放行 */
    public Mono<Void> maybeCompress(RunContext ctx) {
        int window = Math.min(ctx.getLimits().tokenBudget(), 32768);
        int historyBudget = (int) (window * props.getAgent().getTokenBudgetRatio().getHistory());
        int threshold = (int) (historyBudget * props.getAgent().getCompressorTriggerRatio());

        List<ChatMessage> messages = ctx.getMessages();
        int used = messages.stream()
                .mapToInt(m -> TokenEstimator.estimate(m.content()) + 8)
                .sum();
        // 至少保留 3 条才有压缩意义，避免把仅有的上下文也压掉
        if (used < threshold || messages.size() < 3) {
            return Mono.empty();
        }

        return summarizePrompt()
                .flatMap(prompt -> doCompress(ctx, prompt, threshold));
    }

    private Mono<Void> doCompress(RunContext ctx, String prompt, int threshold) {
        List<ChatMessage> messages = ctx.getMessages();
        // 最后一轮（最后一条 user 及其后续消息）原样保留，其余压缩
        int keepFrom = lastTurnStart(messages);
        List<ChatMessage> toCompress = new ArrayList<>(messages.subList(0, keepFrom));
        if (toCompress.isEmpty()) {
            return Mono.empty();
        }

        StringBuilder history = new StringBuilder();
        for (ChatMessage m : toCompress) {
            history.append('[').append(m.role()).append("] ")
                    .append(m.content() == null ? "" : m.content()).append('\n');
        }

        ChatRequest req = new ChatRequest(
                ctx.getModel(),
                List.of(ChatMessage.system(prompt), ChatMessage.user(history.toString())),
                List.of(),
                props.getAgent().getSummaryMaxTokens(),
                0.2);

        return llmInvoker.invoke(req)
                .collectList()
                .map(LlmTurn::merge)
                .doOnNext(turn -> {
                    ctx.addTokenUsed(turn.promptTokens() + turn.completionTokens());
                    String summary = turn.text();
                    if (summary == null || summary.isBlank()) {
                        log.warn("摘要结果为空，放弃本次压缩 runId={}", ctx.getRunId());
                        return;
                    }
                    // 用一条摘要消息替换被压缩的历史，保持剩余消息配对完整
                    messages.subList(0, keepFrom).clear();
                    messages.add(0, ChatMessage.user("【早前对话摘要】" + summary));
                    log.info("上下文压缩完成 runId={} 压缩消息数={} 摘要长度={}",
                            ctx.getRunId(), toCompress.size(), summary.length());
                })
                .then();
    }

    /** 找到最后一轮的起始下标（最后一条 user 消息的位置） */
    private int lastTurnStart(List<ChatMessage> messages) {
        for (int i = messages.size() - 1; i >= 0; i--) {
            if ("user".equals(messages.get(i).role())) {
                return i;
            }
        }
        return messages.size() - 1;
    }

    /** 读取摘要提示词；类路径读取是阻塞 IO，切到 boundedElastic */
    private Mono<String> summarizePrompt() {
        return Mono.fromCallable(() -> {
                    try {
                        var res = new ClassPathResource("prompts/summarize_zh.txt");
                        return new String(res.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        log.warn("加载 prompts/summarize_zh.txt 失败，使用内置兜底提示词: {}", e.toString());
                        return FALLBACK_SUMMARIZE_PROMPT;
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}
