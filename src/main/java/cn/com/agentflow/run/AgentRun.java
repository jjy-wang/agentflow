package cn.com.agentflow.run;

import java.util.Map;

/**
 * t_agent_run 行的只读视图（从 MyBatis Map 结果组装）。
 * 崩溃恢复与状态查询共用，避免在各处直接散落 row.get("...") 字符串。
 */
public record AgentRun(String runId,
                       String tenantId,
                       long sessionId,
                       String mode,
                       String query,
                       int stepIndex,
                       int maxSteps,
                       int tokenBudget,
                       String stateJson) {

    /** 从 RunMapper 的 Map 结果构造；state_json 可能为 null */
    public static AgentRun from(Map<String, Object> row) {
        return new AgentRun(
                String.valueOf(row.get("run_id")),
                String.valueOf(row.get("tenant_id")),
                ((Number) row.get("session_id")).longValue(),
                String.valueOf(row.get("mode")),
                String.valueOf(row.get("query")),
                row.get("step_index") == null ? 0 : ((Number) row.get("step_index")).intValue(),
                row.get("max_steps") == null ? 12 : ((Number) row.get("max_steps")).intValue(),
                row.get("token_budget") == null ? 32000 : ((Number) row.get("token_budget")).intValue(),
                row.get("state_json") == null ? null : String.valueOf(row.get("state_json")));
    }
}
