package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.ToolDef;
import cn.com.agentflow.prompts.PromptAssembler;
import cn.com.agentflow.prompts.PromptPrefixMeter;
import cn.com.agentflow.util.TokenEstimator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 上下文构建：按预算分配 → 整轮裁剪 → 交回 ChatRequest。
 * 注意：裁剪单位是"轮"（user + assistant(+tool)），不能只裁一条，否则工具调用会失去配对。
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class ContextBuilder {

    private final AgentFlowProperties props;
    private final PromptPrefixMeter prefixMeter;

    public ChatRequest build(RunContext ctx, List<ToolDef> tools, String systemPrompt) {
        int window = Math.min(ctx.getLimits().tokenBudget(), 32768);
        AgentFlowProperties.Ratio ratio = props.getAgent().getTokenBudgetRatio();

        int toolBudget = (int) (window * ratio.getTools());
        int historyBudget = (int) (window * ratio.getHistory());
        int outputBudget = (int) (window * ratio.getOutput());

        // 工具 schema 按 name 字典序固定顺序：注册顺序/并发遍历顺序变化不影响前缀字节
        List<ToolDef> sortedTools = PromptAssembler.sortTools(tools);
        List<ToolDef> keptTools = trimTools(sortedTools, toolBudget);
        List<ChatMessage> keptHistory = trimHistory(ctx.getMessages(), historyBudget);

        // 指纹只覆盖稳定前缀（角色说明 + 排序后的工具 schema），记忆/摘要变化不会击穿缓存
        String fingerprint = PromptAssembler.fingerprint(systemPrompt, keptTools);
        boolean reused = prefixMeter.record(fingerprint);
        if (props.getPrompt().isLogFingerprint()) {
            log.debug("prompt 前缀指纹 runId={} fingerprint={} reused={} tools={}",
                    ctx.getRunId(), fingerprint, reused, keptTools.size());
        }

        List<ChatMessage> finalMessages = new ArrayList<>();
        finalMessages.add(ChatMessage.system(systemPrompt));
        finalMessages.addAll(keptHistory);

        return new ChatRequest(ctx.getModel(), finalMessages, keptTools, outputBudget, 0.3);
    }

    private List<ToolDef> trimTools(List<ToolDef> tools, int budget) {
        List<ToolDef> kept = new ArrayList<>();
        int used = 0;
        for (ToolDef t : tools) {
            int cost = TokenEstimator.estimate(t.description()) + 40;
            if (used + cost > budget) {
                log.warn("工具 schema 预算不足，丢弃工具: {}", t.name());
                continue;
            }
            used += cost;
            kept.add(t);
        }
        return kept;
    }

    /** 从最老的开始整轮丢弃，直到满足预算 */
    private List<ChatMessage> trimHistory(List<ChatMessage> all, int budget) {
        List<List<ChatMessage>> turns = toTurns(all);
        int total = turns.stream().mapToInt(this::cost).sum();
        int drop = 0;
        while (total > budget && drop < turns.size()) {
            total -= cost(turns.get(drop));
            drop++;
        }
        List<ChatMessage> kept = new ArrayList<>();
        for (int i = drop; i < turns.size(); i++) {
            kept.addAll(turns.get(i));
        }
        return kept;
    }

    /** 把消息切成轮：user 起始，assistant + 紧随的 tool 结果归为同一轮 */
    private List<List<ChatMessage>> toTurns(List<ChatMessage> messages) {
        List<List<ChatMessage>> turns = new ArrayList<>();
        List<ChatMessage> current = null;
        for (ChatMessage m : messages) {
            if ("user".equals(m.role()) || current == null) {
                current = new ArrayList<>();
                turns.add(current);
            }
            current.add(m);
        }
        return turns;
    }

    private int cost(List<ChatMessage> turn) {
        int c = 0;
        for (ChatMessage m : turn) {
            c += TokenEstimator.estimate(m.content());
            c += 8;
        }
        return c;
    }
}
