package cn.com.agentflow.api;

import cn.com.agentflow.api.AgentController.ApiResult;
import cn.com.agentflow.api.dto.ApprovalRequest;
import cn.com.agentflow.auth.TenantContext;
import cn.com.agentflow.run.ApprovalService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/** HITL 审批回调：人工决定后恢复或终止处于 WAITING_APPROVAL 的 run */
@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/v1/agent/runs")
public class ApprovalController {

    private final ApprovalService approvalService;

    /** 审批通过：run 从挂起步骤继续执行 */
    @PostMapping("/{runId}/approve")
    public Mono<ApiResult<Boolean>> approve(@PathVariable String runId,
                                            @Valid @RequestBody ApprovalRequest req) {
        return TenantContext.current()
                .flatMap(tenantId -> approvalService.decideLatest(tenantId, runId, req.getApprover(), true))
                .map(ApiResult::ok);
    }

    /** 审批拒绝：run 以失败收尾，挂起的工具调用不执行 */
    @PostMapping("/{runId}/reject")
    public Mono<ApiResult<Boolean>> reject(@PathVariable String runId,
                                           @Valid @RequestBody ApprovalRequest req) {
        return TenantContext.current()
                .flatMap(tenantId -> approvalService.decideLatest(tenantId, runId, req.getApprover(), false))
                .map(ApiResult::ok);
    }
}
