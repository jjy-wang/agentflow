package cn.com.agentflow.api;

import cn.com.agentflow.api.AgentController.ApiResult;
import cn.com.agentflow.auth.TenantContext;
import cn.com.agentflow.run.RunService;
import cn.com.agentflow.run.RunState;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/** 运行状态查询：SSE 断线期间客户端的轮询兜底入口 */
@RequiredArgsConstructor
@RestController
@RequestMapping("/v1/agent/runs")
public class RunStatusController {

    private final RunService runService;

    @GetMapping("/{runId}/status")
    public Mono<ApiResult<RunState>> status(@PathVariable String runId) {
        return TenantContext.current()
                .flatMap(tenantId -> runService.status(tenantId, runId))
                .map(ApiResult::ok);
    }
}
