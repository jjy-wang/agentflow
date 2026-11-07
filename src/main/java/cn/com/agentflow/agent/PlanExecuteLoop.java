package cn.com.agentflow.agent;

import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ChatChunk;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.LlmInvoker;
import cn.com.agentflow.stream.EventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

/**
 * 计划-执行模式：先让模型产出一份执行计划（不带工具），
 * 再把计划注入系统提示词，交给 ReActLoop 逐步执行。
 * 工具调用、死循环防护均由 ReActLoop / LoopGuard 复用保证。
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class PlanExecuteLoop {

    /** 规划阶段追加的系统指令：只输出计划，不调用工具 */
    private static final String PLAN_DIRECTIVE =
            "\n\n【模式】现在是计划阶段。请先输出一份编号执行计划（每行一步，说明目标与将使用的工具），"
                    + "不要输出计划以外的内容，本阶段不会执行任何工具。";

    private final LlmInvoker llmInvoker;
    private final ReActLoop reActLoop;
    private final LoopGuard loopGuard;

    public Mono<String> run(RunContext ctx, String systemPrompt) {
        return makePlan(ctx, systemPrompt)
                .flatMap(plan -> {
                    ctx.append(ChatMessage.assistant("【执行计划】\n" + plan, null));
                    String execPrompt = systemPrompt
                            + "\n\n【已确认的执行计划】\n" + plan
                            + "\n请严格按照上述计划逐步执行，计划已全部完成后给出最终答复。";
                    // RUN_STARTED 由 ReActLoop 统一发出，避免重复事件
                    return reActLoop.run(ctx, execPrompt);
                });
    }

    /** 规划轮：LoopGuard 检查 → 流式生成计划 → 聚合计费 */
    private Mono<String> makePlan(RunContext ctx, String systemPrompt) {
        return Mono.defer(() -> {
            var violation = loopGuard.check(ctx);
            if (violation.isPresent()) {
                return Mono.error(new AgentException(violation.get().code(), violation.get().message()));
            }

            ctx.nextStep();
            ctx.emit(EventType.STEP_STARTED, "{\"step\":" + ctx.getStep() + ",\"kind\":\"PLAN\"}");

            List<ChatMessage> messages = new ArrayList<>();
            messages.add(ChatMessage.system(systemPrompt + PLAN_DIRECTIVE));
            messages.addAll(ctx.getMessages());
            // 规划阶段输出较短，固定给一个适中的输出预算
            ChatRequest req = new ChatRequest(ctx.getModel(), messages, List.of(), 2048, 0.2);

            return llmInvoker.invoke(req)
                    .doOnNext(chunk -> {
                        if (chunk instanceof ChatChunk.TextDelta t) {
                            ctx.emit(EventType.LLM_DELTA, "{\"text\":\"" + escape(t.text()) + "\"}");
                        }
                    })
                    .collectList()
                    .map(chunks -> {
                        LlmTurn turn = LlmTurn.merge(chunks);
                        ctx.addTokenUsed(turn.promptTokens() + turn.completionTokens());
                        ctx.addPromptTokens(turn.promptTokens());
                        String plan = turn.text();
                        if (plan == null || plan.isBlank()) {
                            throw new AgentException("PLAN_EMPTY", "模型未产出有效执行计划");
                        }
                        log.info("计划生成完成 runId={} 计划长度={}", ctx.getRunId(), plan.length());
                        return plan;
                    });
        });
    }

    private String escape(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "");
    }
}
