package cn.com.agentflow.run;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.mapper.RunMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 每 5 秒扫一次过期租约，抢过来重新执行。
 * 已完成的步骤不重跑（靠 state_json 里的 step_index），
 * 正在执行的步骤会重跑，所以工具必须幂等。
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class RunRecoveryJob {

    private final RunMapper runMapper;
    private final RunService runService;
    private final ApprovalService approvalService;
    private final AgentFlowProperties props;

    @Value("${agentflow.instance-id:local}")
    private String instanceId;

    @Scheduled(fixedDelayString = "${agentflow.run.recovery-interval-ms:5000}")
    public void recover() {
        // 审批超时收敛复用本调度周期，不新建调度器；挂起 run 不在 claimExpired 范围内
        approvalService.expireStale(props.getRun().getRecoveryBatch()).block();
        List<Map<String, Object>> rows = runMapper.claimExpired(
                instanceId, props.getRun().getLeaseSeconds(), props.getRun().getRecoveryBatch());
        if (rows.isEmpty()) {
            return;
        }
        log.warn("接管过期 run 数量: {}", rows.size());
        rows.forEach(row -> {
            String runId = String.valueOf(row.get("run_id"));
            log.info("接管 run: {}", runId);
            runService.resume(runId, row);
        });
    }
}
