package cn.com.agentflow.run;

import cn.com.agentflow.agent.AgentState;
import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.stream.EventType;
import cn.com.agentflow.tool.ToolMeta;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 人机协同审批（HITL）：高风险工具让 run 进入 WAITING_APPROVAL 挂起态，
 * 审批通过后由 RunService 从 state_json 精确恢复并补执行挂起的工具调用，驳回则转 CANCELED。
 * 所有 JDBC 阻塞操作统一切到 boundedElastic。
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class ApprovalService {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final AgentFlowProperties props;
    /** 延迟取 RunService，避免 RunService → AgentRuntime → ReActLoop → ToolExecutor → ApprovalService 的构造期循环依赖 */
    private final ObjectProvider<RunService> runServiceProvider;

    /**
     * 写审批单并返回 approvalId；(run_id, step_index) 唯一，重复挂起同一位置复用原单。
     * args_json 必须是合法 JSON：模型幻觉参数在这里先做兜底归一化，避免 ::jsonb 转换失败。
     */
    public Mono<Long> requestApproval(RunContext ctx, ToolCall call, ToolMeta meta) {
        return Mono.fromCallable(() -> jdbc.queryForObject(
                        "INSERT INTO t_approval(run_id, step_index, tool_name, args_json) "
                                + "VALUES (?,?,?,?::jsonb) "
                                + "ON CONFLICT (run_id, step_index) DO UPDATE SET tool_name = t_approval.tool_name "
                                + "RETURNING id",
                        Long.class, ctx.getRunId(), ctx.getStep(), call.name(),
                        jsonOrEmpty(call.argumentsJson())))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 挂起 run：持久化完整状态快照（含待执行工具批次与事件序号）+ 释放租约 + 发 run.waiting_approval。
     * 事件带工具名、参数、callId 与整批待执行调用，前端据此渲染审批卡片。
     */
    public Mono<Void> suspend(RunContext ctx, Long approvalId) {
        return Mono.fromRunnable(() -> {
            ctx.emit(EventType.WAITING_APPROVAL, waitingPayload(ctx, approvalId));
            String stateJson = AgentState.from(ctx).toJson(mapper);
            jdbc.update("UPDATE t_agent_run SET state='WAITING_APPROVAL', step_index=?, token_used=?, "
                            + "state_json=?::jsonb, lease_until=now() "
                            + "WHERE run_id=? AND state='RUNNING'",
                    ctx.getStep(), ctx.getTokenUsed(), stateJson, ctx.getRunId());
            log.info("run 挂起等待审批 runId={} step={} approvalId={} pending={}",
                    ctx.getRunId(), ctx.getStep(), approvalId, ctx.getPendingToolCalls().size());
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    /**
     * 审批裁决：t_approval 与 t_agent_run 都用 CAS 条件更新，重复审批/并发审批只有第一次生效。
     * 通过 → run 置 PENDING（清租约）并立即在本实例拉起续跑；驳回 → run 置 CANCELED 并释放资源。
     */
    public Mono<Void> decide(String runId, int stepIndex, String approver, boolean approve) {
        return Mono.fromRunnable(() -> {
            int updated = jdbc.update("UPDATE t_approval SET status=?, approver=?, decided_at=now() "
                            + "WHERE run_id=? AND step_index=? AND status='PENDING'",
                    approve ? "APPROVED" : "REJECTED", approver, runId, stepIndex);
            if (updated == 0) {
                throw new AgentException("APPROVAL_NOT_FOUND", "审批单不存在或已裁决");
            }
            if (approve) {
                int moved = jdbc.update("UPDATE t_agent_run SET state='PENDING', lease_owner=NULL, "
                                + "lease_until=NULL, error_code=NULL, error_msg=NULL "
                                + "WHERE run_id=? AND state='WAITING_APPROVAL'", runId);
                if (moved == 0) {
                    throw new AgentException("APPROVAL_STATE_CONFLICT", "run 不在待审批状态，审批已失效");
                }
                // 立即续跑，不依赖恢复任务的扫描周期；CAS 由 RunService 内部完成
                runServiceProvider.getObject().resumeAfterApproval(runId);
            } else {
                int canceled = jdbc.update("UPDATE t_agent_run SET state='CANCELED', "
                                + "error_code='APPROVAL_REJECTED', error_msg='审批驳回', "
                                + "finished_at=now(), lease_until=now() "
                                + "WHERE run_id=? AND state='WAITING_APPROVAL'", runId);
                if (canceled == 0) {
                    throw new AgentException("APPROVAL_STATE_CONFLICT", "run 不在待审批状态，审批已失效");
                }
                jdbc.update("UPDATE t_approval SET status='REJECTED', decided_at=now() "
                        + "WHERE run_id=? AND status='PENDING'", runId);
                runServiceProvider.getObject().rejectAfterApproval(runId);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    /** 按 run 最新一条待审批单裁决（Controller 入口）：先校验租户归属，越权审批直接拒绝 */
    public Mono<Boolean> decideLatest(String tenantId, String runId, String approver, boolean approve) {
        return Mono.fromCallable(() -> {
                    Integer owned = jdbc.queryForObject(
                            "SELECT count(*) FROM t_agent_run WHERE run_id=? AND tenant_id=?::bigint",
                            Integer.class, runId, tenantId);
                    if (owned == null || owned == 0) {
                        throw new AgentException("RUN_NOT_FOUND", "run 不存在或无权限");
                    }
                    try {
                        return jdbc.queryForObject(
                                "SELECT step_index FROM t_approval WHERE run_id=? AND status='PENDING' "
                                        + "ORDER BY step_index DESC LIMIT 1",
                                Integer.class, runId);
                    } catch (EmptyResultDataAccessException e) {
                        throw new AgentException("APPROVAL_NOT_FOUND", "审批单不存在或已裁决");
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                // 惰性超时判定：审批入口读取时先收敛已超时未处理的审批单
                .flatMap(step -> expireIfStale(runId).then(decide(runId, step, approver, approve)))
                .thenReturn(true);
    }

    /** 查询 run 当前待审批记录（供审批列表页） */
    public Mono<List<Map<String, Object>>> pendingOf(String runId) {
        return Mono.fromCallable(() -> jdbc.queryForList(
                        "SELECT run_id, step_index, tool_name, args_json::text AS args_json, status, created_at "
                                + "FROM t_approval WHERE run_id=? ORDER BY step_index", runId))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 对账式超时收敛：扫描超过 agentflow.approval.timeout 仍未裁决的审批单，
     * 把审批单置 TIMEOUT、run 置 TIMEOUT 并释放租约；批内逐条 CAS，多实例并发不会重复处理。
     */
    public Mono<Integer> expireStale(int batch) {
        return Mono.fromCallable(() -> {
                    List<String> runIds = jdbc.queryForList(
                            "SELECT a.run_id FROM t_approval a "
                                    + "JOIN t_agent_run r ON r.run_id = a.run_id "
                                    + "WHERE a.status='PENDING' AND r.state='WAITING_APPROVAL' "
                                    + "AND a.created_at < now() - (? || ' millisecond')::interval "
                                    + "ORDER BY a.created_at LIMIT ?",
                            String.class, timeoutMs(), Math.max(1, batch));
                    int expired = 0;
                    for (String runId : runIds) {
                        if (expireOne(runId)) {
                            expired++;
                        }
                    }
                    if (expired > 0) {
                        log.warn("审批超时自动收敛 run 数量: {}", expired);
                    }
                    return expired;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /** 单 run 惰性超时判定：命中则收敛并抛 APPROVAL_TIMEOUT，避免审批入口继续接受过期裁决 */
    private Mono<Void> expireIfStale(String runId) {
        return Mono.fromRunnable(() -> {
            Integer stale = jdbc.queryForObject(
                    "SELECT count(*) FROM t_approval a "
                            + "JOIN t_agent_run r ON r.run_id = a.run_id "
                            + "WHERE a.run_id=? AND a.status='PENDING' AND r.state='WAITING_APPROVAL' "
                            + "AND a.created_at < now() - (? || ' millisecond')::interval",
                    Integer.class, runId, timeoutMs());
            if (stale != null && stale > 0 && expireOne(runId)) {
                throw new AgentException("APPROVAL_TIMEOUT", "审批超时未处理，run 已转 TIMEOUT");
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    /** 收敛单个 run；返回 run 状态是否由本次成功迁移为 TIMEOUT（CAS 保证只生效一次） */
    private boolean expireOne(String runId) {
        jdbc.update("UPDATE t_approval SET status='TIMEOUT', decided_at=now() "
                + "WHERE run_id=? AND status='PENDING'", runId);
        int updated = jdbc.update("UPDATE t_agent_run SET state='TIMEOUT', error_code='APPROVAL_TIMEOUT', "
                        + "error_msg='审批超时未处理', finished_at=now(), lease_until=now() "
                        + "WHERE run_id=? AND state='WAITING_APPROVAL'", runId);
        if (updated > 0) {
            runServiceProvider.getObject().expireAfterApproval(runId);
            return true;
        }
        return false;
    }

    private long timeoutMs() {
        return Math.max(1, props.getApproval().getTimeout().toMillis());
    }

    /** 挂起事件的完整上下文：工具名、参数、callId，以及整批待执行调用 */
    private String waitingPayload(RunContext ctx, Long approvalId) {
        try {
            ToolCall first = ctx.getPendingToolCalls().isEmpty() ? null : ctx.getPendingToolCalls().get(0);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("runId", ctx.getRunId());
            payload.put("state", RunState.WAITING_APPROVAL.name());
            payload.put("step", ctx.getStep());
            payload.put("approvalId", approvalId);
            if (first != null) {
                payload.put("tool", first.name());
                payload.put("callId", first.id());
                payload.put("args", mapper.readTree(jsonOrEmpty(first.argumentsJson())));
            }
            List<Map<String, Object>> pending = new ArrayList<>();
            for (ToolCall call : ctx.getPendingToolCalls()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("callId", call.id());
                item.put("name", call.name());
                item.put("args", mapper.readTree(jsonOrEmpty(call.argumentsJson())));
                pending.add(item);
            }
            payload.put("pending", pending);
            return mapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.warn("审批事件序列化失败，降级为最小 payload runId={} err={}", ctx.getRunId(), e.toString());
            return "{\"runId\":\"" + ctx.getRunId() + "\",\"state\":\"WAITING_APPROVAL\"}";
        }
    }

    private String jsonOrEmpty(String raw) {
        try {
            JsonNode node = mapper.readTree(raw == null || raw.isBlank() ? "{}" : raw);
            return node == null ? "{}" : node.toString();
        } catch (Exception e) {
            return "{}";
        }
    }
}
