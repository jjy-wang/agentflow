package cn.com.agentflow.run;

import cn.com.agentflow.agent.AgentRuntime;
import cn.com.agentflow.agent.AgentState;
import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.agent.RunLimits;
import cn.com.agentflow.api.dto.RunRequest;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.mapper.RunMapper;
import cn.com.agentflow.metric.AgentMetrics;
import cn.com.agentflow.stream.EventType;
import cn.com.agentflow.stream.RunEventBus;
import cn.com.agentflow.stream.SseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * run 生命周期：创建 → 租约抢占 → 执行 → 落库结束。
 * 执行在独立订阅链上跑，不占用 HTTP 连接；崩溃后由 RunRecoveryJob 接管并调用 resume 续跑。
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class RunService {

    private static final String DEFAULT_MODE = "REACT";

    private final RunMapper runMapper;
    private final AgentRuntime agentRuntime;
    private final RunEventBus eventBus;
    private final AgentMetrics metrics;
    private final AgentFlowProperties props;
    private final ObjectMapper mapper;

    private final ConcurrentHashMap<String, Disposable> running = new ConcurrentHashMap<>();
    /** 本实例挂起的 run：保留 SSE sink 与最后事件序号，审批通过后原地继续推送 */
    private final ConcurrentHashMap<String, SuspendedRun> suspendedRuns = new ConcurrentHashMap<>();

    @Value("${agentflow.instance-id:local}")
    private String instanceId;

    public Mono<AgentRunView> create(String tenantId, RunRequest req) {
        return Mono.fromCallable(() -> {
                    String runId = UUID.randomUUID().toString().replace("-", "").substring(0, 24);
                    int maxSteps = req.getMaxSteps() == null ? props.getAgent().getMaxSteps() : req.getMaxSteps();
                    int budget = props.getAgent().getDefaultTokenBudget();
                    // sessionId 允许缺省（README/压测脚本均不带），避免 Long 拆箱 NPE 导致 500
                    long sessionId = req.getSessionId() == null ? 0L : req.getSessionId();
                    runMapper.insert(runId, tenantId, sessionId, DEFAULT_MODE,
                            req.getQuery(), maxSteps, budget);
                    return new AgentRunView(runId, tenantId, sessionId, maxSteps, budget);
                })
                .subscribeOn(Schedulers.boundedElastic())
                // 直接构造好的 view 原样返回；副作用（启动执行）放 map 里同步完成
                .map(view -> {
                    start(view, req);
                    return view;
                });
    }

    /** 真正执行：在独立线程池跑，不占用 HTTP 连接 */
    private void start(AgentRunView view, RunRequest req) {
        Sinks.Many<SseEvent> sink = eventBus.create(view.runId());
        RunContext ctx = newContext(view.tenantId(), view.runId(), view.sessionId(), DEFAULT_MODE,
                req.getModel() == null ? props.getLlm().getDefaultModel() : req.getModel(),
                view.maxSteps(), view.tokenBudget(), sink);
        ctx.append(ChatMessage.user(req.getQuery()));

        int claimed = runMapper.casState(view.runId(), RunState.PENDING, RunState.RUNNING,
                instanceId, props.getRun().getLeaseSeconds());
        if (claimed == 0) {
            log.warn("run 抢占失败，可能已被其他实例处理: {}", view.runId());
            eventBus.complete(view.runId());
            return;
        }
        execute(view.runId(), ctx);
    }

    /**
     * 崩溃恢复续跑：从 state_json 反序列化 AgentState，回灌历史消息后按 mode 续跑。
     * claimExpired 已完成租约抢占，这里不再 casState。
     * 已完成的步骤靠 messages 里的历史跳过；正在执行的步骤会重跑，工具必须幂等。
     */
    public void resume(String runId, Map<String, Object> row) {
        try {
            AgentRun run = AgentRun.from(row);
            Sinks.Many<SseEvent> sink = eventBus.create(runId);
            RunContext ctx = newContext(run.tenantId(), runId, run.sessionId(), run.mode(),
                    props.getLlm().getDefaultModel(), run.maxSteps(), run.tokenBudget(), sink);

            boolean hasPendingTools = false;
            if (run.stateJson() != null && !run.stateJson().isBlank() && !"{}".equals(run.stateJson())) {
                // 断点续跑：历史消息回灌，模型据此感知已完成的工具调用
                AgentState snapshot = AgentState.fromJson(mapper, run.stateJson());
                snapshot.restoreMessagesInto(ctx);
                // 步数/工具调用/token 计数必须一起恢复，否则续跑会绕过 LoopGuard 预算
                ctx.restoreProgress(snapshot.step(), snapshot.toolCallCount(),
                        snapshot.tokenUsed(), snapshot.promptTokensTotal());
                ctx.restoreEventSeq(snapshot.eventSeq());
                ctx.suspend(snapshot.pendingToolCalls());
                hasPendingTools = !snapshot.pendingToolCalls().isEmpty();
            } else if (run.query() != null && !"null".equals(run.query())) {
                // 还没有任何快照（PENDING 就被接管）：等价于从头开始
                ctx.append(ChatMessage.user(run.query()));
            }

            if (ctx.getMessages().isEmpty()) {
                log.error("run 续跑上下文为空，直接失败: runId={}", runId);
                runMapper.finish(runId, RunState.FAILED, "STATE_EMPTY", "续跑上下文为空",
                        0, run.stepIndex(), "{}");
                eventBus.complete(runId);
                return;
            }
            log.info("run 续跑 runId={} mode={} 从 step={} 恢复消息数={} 待补执行工具数={}",
                    runId, run.mode(), run.stepIndex(), ctx.getMessages().size(),
                    ctx.getPendingToolCalls().size());
            // 审批通过后被恢复任务抢先认领：仍需先补执行挂起批次
            if (hasPendingTools) {
                executeResume(runId, ctx);
            } else {
                execute(runId, ctx);
            }
        } catch (Exception e) {
            log.error("run 接管失败 runId={}", runId, e);
            runMapper.finish(runId, RunState.FAILED, "RESUME_ERROR", e.getMessage(), 0, 0, "{}");
            eventBus.complete(runId);
        }
    }

    /** 组装运行上下文（创建与续跑共用） */
    private RunContext newContext(String tenantId, String runId, long sessionId, String mode,
                                  String model, int maxSteps, int tokenBudget,
                                  Sinks.Many<SseEvent> sink) {
        RunLimits limits = new RunLimits(maxSteps, props.getAgent().getMaxToolCalls(),
                props.getAgent().getMaxWallClockMs(), tokenBudget);
        return new RunContext(tenantId, runId, sessionId, mode, model, limits, sink);
    }

    /** 启动执行链并把订阅句柄登记到 running，供 cancel 级联取消 */
    private void execute(String runId, RunContext ctx) {
        subscribe(runId, ctx, agentRuntime.execute(ctx));
    }

    /** 审批通过后续跑：跳过已成功步骤，先补执行挂起批次 */
    private void executeResume(String runId, RunContext ctx) {
        subscribe(runId, ctx, agentRuntime.executeResume(ctx));
    }

    private void subscribe(String runId, RunContext ctx, Mono<String> chain) {
        var disposable = chain.subscribe(
                text -> finish(runId, ctx, RunState.SUCCEEDED, null, null),
                err -> {
                    String code = err instanceof AgentException ae ? ae.getCode() : "INTERNAL_ERROR";
                    RunState state = isTimeout(code) ? RunState.TIMEOUT : RunState.FAILED;
                    finish(runId, ctx, state, code, err.getMessage());
                },
                () -> {
                    if (ctx.isSuspended()) {
                        // 挂起是暂停不是结束：保留 SSE sink 与事件序号，审批后原地续推，
                        // 不落终态、不发 run.failed，run 状态由 ApprovalService 置为 WAITING_APPROVAL
                        suspendedRuns.put(runId, new SuspendedRun(ctx.getSink(), ctx.lastEventSeq()));
                        log.info("run 已挂起等待人工审批 runId={} step={} pending={}",
                                runId, ctx.getStep(), ctx.getPendingToolCalls().size());
                        return;
                    }
                    eventBus.complete(runId);
                });
        running.put(runId, disposable);
        // 同步完成（挂起/快速终态）时订阅已释放，不在 running 留空句柄
        if (disposable.isDisposed()) {
            running.remove(runId);
        }
    }

    /**
     * 审批通过后的立即续跑（ApprovalService 调用）：PENDING → RUNNING 用 CAS 抢占，
     * 抢不到说明已被恢复任务或其他实例处理，直接放弃避免双跑。
     * 从 state_json 恢复消息、进度计数、事件序号与待执行工具批次，再补执行挂起工具。
     */
    public void resumeAfterApproval(String runId) {
        int claimed = runMapper.casState(runId, RunState.PENDING, RunState.RUNNING,
                instanceId, props.getRun().getLeaseSeconds());
        if (claimed == 0) {
            log.warn("审批续跑抢占失败，已由其他执行体接管: runId={}", runId);
            return;
        }
        Map<String, Object> row = runMapper.find(runId);
        if (row == null) {
            log.error("审批续跑读取 run 失败: runId={}", runId);
            return;
        }
        AgentRun run = AgentRun.from(row);
        SuspendedRun suspended = suspendedRuns.remove(runId);
        Sinks.Many<SseEvent> sink = suspended == null ? eventBus.create(runId) : suspended.sink();
        RunContext ctx = newContext(run.tenantId(), runId, run.sessionId(), run.mode(),
                props.getLlm().getDefaultModel(), run.maxSteps(), run.tokenBudget(), sink);

        List<ToolCall> pending = List.of();
        if (run.stateJson() != null && !run.stateJson().isBlank() && !"{}".equals(run.stateJson())) {
            AgentState snapshot = AgentState.fromJson(mapper, run.stateJson());
            snapshot.restoreMessagesInto(ctx);
            ctx.restoreProgress(snapshot.step(), snapshot.toolCallCount(),
                    snapshot.tokenUsed(), snapshot.promptTokensTotal());
            ctx.restoreEventSeq(snapshot.eventSeq());
            pending = snapshot.pendingToolCalls();
            ctx.suspend(pending);
        } else if (run.query() != null && !"null".equals(run.query())) {
            ctx.append(ChatMessage.user(run.query()));
        }
        if (ctx.getMessages().isEmpty()) {
            runMapper.finish(runId, RunState.FAILED, "STATE_EMPTY", "续跑上下文为空",
                    0, run.stepIndex(), "{}");
            eventBus.complete(runId);
            return;
        }
        log.info("审批通过，run 续跑 runId={} step={} messages={} pendingTools={}",
                runId, ctx.getStep(), ctx.getMessages().size(), pending.size());
        executeResume(runId, ctx);
    }

    /** 审批驳回后的收尾：状态已由 ApprovalService CAS 为 CANCELED，这里发终态事件并关闭 SSE */
    public void rejectAfterApproval(String runId) {
        running.remove(runId);
        SuspendedRun suspended = suspendedRuns.remove(runId);
        if (suspended != null) {
            suspended.sink().tryEmitNext(new SseEvent(suspended.lastSeq() + 1, EventType.RUN_CANCELED,
                    "{\"state\":\"CANCELED\",\"code\":\"APPROVAL_REJECTED\",\"reason\":\"审批驳回\"}"));
        }
        eventBus.complete(runId);
        metrics.runFinished(modeOf(runId), RunState.CANCELED.name(), 0);
        log.info("审批驳回，run 已取消 runId={}", runId);
    }

    /** 审批超时的收尾：状态已由 ApprovalService CAS 为 TIMEOUT，这里发终态事件并释放资源 */
    public void expireAfterApproval(String runId) {
        running.remove(runId);
        SuspendedRun suspended = suspendedRuns.remove(runId);
        if (suspended != null) {
            suspended.sink().tryEmitNext(new SseEvent(suspended.lastSeq() + 1, EventType.RUN_FAILED,
                    "{\"state\":\"TIMEOUT\",\"code\":\"APPROVAL_TIMEOUT\"}"));
        }
        eventBus.complete(runId);
        metrics.runFinished(modeOf(runId), RunState.TIMEOUT.name(), 0);
        log.warn("审批超时，run 已转 TIMEOUT 并释放资源 runId={}", runId);
    }

    /** 终态事件需要 mode 标签：读不到行时退回默认模式，不影响主流程 */
    private String modeOf(String runId) {
        try {
            Map<String, Object> row = runMapper.find(runId);
            return row == null ? DEFAULT_MODE : String.valueOf(row.get("mode"));
        } catch (Exception e) {
            return DEFAULT_MODE;
        }
    }

    /** 超时类错误码统一落 TIMEOUT 状态，其余按 FAILED 处理 */
    private static boolean isTimeout(String code) {
        return "WALL_CLOCK_TIMEOUT".equals(code)
                || "RUN_TIMEOUT".equals(code)
                || "STEP_TIMEOUT".equals(code)
                || "APPROVAL_TIMEOUT".equals(code);
    }

    /**
     * 统一收尾：落库（finish 同时把 lease_until 置为 now() 释放租约）→ 发终态事件 → 关闭 SSE sink。
     * 成功、失败、超时复用同一条路径，超时不另起一套资源回收；
     * 配额由 LLM 层 doFinally 按已用量结算，这里只负责 run 级收尾。
     */
    private void finish(String runId, RunContext ctx, RunState state, String errorCode, String error) {
        running.remove(runId);
        // state_json 落库：中途失败也要能断点续跑
        String stateJson = AgentState.from(ctx).toJson(mapper);
        String dbCode = state == RunState.SUCCEEDED ? null
                : (errorCode == null ? "RUN_" + state : errorCode);
        runMapper.finish(runId, state, dbCode, error,
                ctx.getTokenUsed(), ctx.getStep(), stateJson);
        StringBuilder payload = new StringBuilder("{\"state\":\"").append(state)
                .append("\",\"tokens\":").append(ctx.getTokenUsed());
        if (state != RunState.SUCCEEDED) {
            if (errorCode != null) {
                payload.append(",\"code\":\"").append(errorCode).append('"');
            }
            payload.append(",\"elapsedMs\":").append(ctx.elapsedMs());
        }
        payload.append('}');
        ctx.emit(state == RunState.SUCCEEDED ? EventType.RUN_SUCCEEDED : EventType.RUN_FAILED,
                payload.toString());
        eventBus.complete(runId);
        metrics.runFinished(ctx.getMode(), state.name(), ctx.elapsedMs());
        log.info("run 结束 runId={} state={} code={} steps={} tokens={} cost={}ms",
                runId, state, errorCode, ctx.getStep(), ctx.getTokenUsed(), ctx.elapsedMs());
    }

    public Mono<Void> cancel(String runId, String reason) {
        return Mono.fromRunnable(() -> {
            var d = running.remove(runId);
            if (d != null) {
                d.dispose();   // 级联取消：关闭模型流
            }
            suspendedRuns.remove(runId);
            runMapper.finish(runId, RunState.CANCELED, "CANCELED", reason, 0, 0, "{}");
            eventBus.complete(runId);
            metrics.runFinished(DEFAULT_MODE, "CANCELED", 0);
            log.info("run 被取消 runId={} reason={}", runId, reason);
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    public Mono<Boolean> cancelAndConfirm(String tenantId, String runId) {
        return assertOwner(tenantId, runId)
                .then(cancel(runId, "USER_CANCELED"))
                .thenReturn(true);
    }

    public Mono<Void> assertOwner(String tenantId, String runId) {
        return Mono.fromCallable(() -> runMapper.find(runId))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(row -> {
                    if (row == null || !tenantId.equals(String.valueOf(row.get("tenant_id")))) {
                        return Mono.error(new AgentException("RUN_NOT_FOUND", "run 不存在"));
                    }
                    return Mono.empty();
                });
    }

    public Mono<RunState> status(String tenantId, String runId) {
        return assertOwner(tenantId, runId)
                .then(Mono.fromCallable(() -> {
                    var row = runMapper.find(runId);
                    return RunState.valueOf(String.valueOf(row.get("state")));
                }).subscribeOn(Schedulers.boundedElastic()));
    }

    /** 调用方终止时释放本实例填写的挂起资源 */
    private record SuspendedRun(Sinks.Many<SseEvent> sink, long lastSeq) {
    }

    public record AgentRunView(String runId, String tenantId, long sessionId,
                               int maxSteps, int tokenBudget) {
    }
}
