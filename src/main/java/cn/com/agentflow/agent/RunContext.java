package cn.com.agentflow.agent;

import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.stream.SseEvent;
import lombok.Getter;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 一次 run 的可变上下文。消息列表只在编排线程上追加；
 * 工具计数、重复调用指纹与事件发射需支持同轮并行工具并发访问，故做原子化/串行化。
 * 不跨实例共享（跨实例靠数据库 state_json）。
 */
@Getter
public class RunContext {

    private final String tenantId;
    private final String runId;
    private final long sessionId;
    private final String mode;
    private final String model;
    private final RunLimits limits;
    private final long startedAt = System.currentTimeMillis();

    private final List<ChatMessage> messages = new ArrayList<>();
    private final Map<String, Integer> fingerprints = new ConcurrentHashMap<>();
    /** 已获批的工具调用 id：审批通过后续跑时跳过再审批，避免同一批调用反复挂起 */
    private final Set<String> approvedCallIds = ConcurrentHashMap.newKeySet();
    private final Sinks.Many<SseEvent> sink;
    private final AtomicLong seq = new AtomicLong();

    private int step;
    /** 同轮工具并行时会被多个执行线程同时自增 */
    private final AtomicInteger toolCallCount = new AtomicInteger();
    private int tokenUsed;
    private int promptTokensTotal;
    /** 结构化输出协议校验失败后的回灌重试次数（单 run 维度） */
    private int protocolRetries;
    /** 连续单步超时次数：任一步正常完成即清零，用于「最多重试 1 次」的判定 */
    private int consecutiveStepTimeouts;
    /** 挂起待审批的工具调用批次：随 state_json 落库，审批通过后据此精确补执行 */
    private List<ToolCall> pendingToolCalls = List.of();
    /** 本轮是否因等待人工审批而挂起：挂起是正常暂停，不是失败 */
    private volatile boolean suspended;

    public RunContext(String tenantId, String runId, long sessionId, String mode, String model,
                      RunLimits limits, Sinks.Many<SseEvent> sink) {
        this.tenantId = tenantId;
        this.runId = runId;
        this.sessionId = sessionId;
        this.mode = mode;
        this.model = model;
        this.limits = limits;
        this.sink = sink;
    }

    /** 并行工具会从多个线程发事件：串行化后 seq 与 Sink 发射顺序保持一致 */
    public synchronized void emit(String type, String data) {
        sink.tryEmitNext(new SseEvent(seq.incrementAndGet(), type, data));
    }

    public void append(ChatMessage message) {
        messages.add(message);
    }

    public void nextStep() {
        step++;
    }

    public int getToolCallCount() {
        return toolCallCount.get();
    }

    public void incToolCall() {
        toolCallCount.incrementAndGet();
    }

    public void incProtocolRetry() {
        protocolRetries++;
    }

    /** 记录一次单步超时并返回累计的连续次数 */
    public int incConsecutiveStepTimeouts() {
        return ++consecutiveStepTimeouts;
    }

    public void resetConsecutiveStepTimeouts() {
        consecutiveStepTimeouts = 0;
    }

    public void addTokenUsed(int tokens) {
        tokenUsed += Math.max(0, tokens);
    }

    public void addPromptTokens(int tokens) {
        promptTokensTotal += Math.max(0, tokens);
    }

    /** 记录挂起时本轮待执行的工具调用（含需要审批的那个），用于 state_json 断点恢复 */
    public void suspend(List<ToolCall> calls) {
        this.pendingToolCalls = List.copyOf(calls);
    }

    public void clearPendingToolCalls() {
        this.pendingToolCalls = List.of();
    }

    /** 标记本轮因审批挂起：ReActLoop 据此以正常完成信号结束，而不是走失败收尾 */
    public void markSuspended() {
        this.suspended = true;
    }

    public boolean isSuspended() {
        return suspended;
    }

    /** 审批通过后续跑：清掉挂起标记，后续失败/成功语义恢复正常 */
    public void clearSuspended() {
        this.suspended = false;
    }

    public void markCallApproved(String callId) {
        if (callId != null) {
            approvedCallIds.add(callId);
        }
    }

    public boolean isCallApproved(String callId) {
        return callId != null && approvedCallIds.contains(callId);
    }

    /** 已发出的最大事件序号：续跑复用同一 run 的事件序列，避免 t_run_event 主键冲突丢事件 */
    public long lastEventSeq() {
        return seq.get();
    }

    public void restoreEventSeq(long value) {
        seq.set(Math.max(0, value));
    }

    /** 恢复步数/工具调用/计费计数：续跑不能从 0 重新计数，否则绕过 LoopGuard 预算 */
    public void restoreProgress(int step, int toolCalls, int tokens, int promptTokens) {
        this.step = Math.max(0, step);
        this.toolCallCount.set(Math.max(0, toolCalls));
        this.tokenUsed = Math.max(0, tokens);
        this.promptTokensTotal = Math.max(0, promptTokens);
    }

    /** 记录 (工具名称 + 规范化参数) 指纹，返回该指纹出现次数；merge 原子执行，并发下计数不漏 */
    public int fingerprint(String key) {
        return fingerprints.merge(key, 1, Integer::sum);
    }

    public long elapsedMs() {
        return System.currentTimeMillis() - startedAt;
    }

    public long remainingMs() {
        return Math.max(0, limits.maxWallClockMs() - elapsedMs());
    }

    public boolean budgetExhausted() {
        return tokenUsed >= limits.tokenBudget();
    }
}
