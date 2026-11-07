package cn.com.agentflow.mapper;

import cn.com.agentflow.run.RunState;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;

/** t_agent_run 状态机 Mapper：关键 SQL 用注解，含租约抢占 */
@Mapper
public interface RunMapper {

    @Insert("INSERT INTO t_agent_run(run_id, tenant_id, session_id, mode, state, query, max_steps, token_budget) "
            + "VALUES (#{runId}, #{tenantId}::bigint, #{sessionId}, #{mode}, 'PENDING', #{query}, #{maxSteps}, #{tokenBudget})")
    int insert(@Param("runId") String runId, @Param("tenantId") String tenantId,
               @Param("sessionId") long sessionId, @Param("mode") String mode,
               @Param("query") String query, @Param("maxSteps") int maxSteps,
               @Param("tokenBudget") int tokenBudget);

    /** 条件更新：只有当前状态匹配才成功，返回 0 说明被别人抢走 */
    @Update("UPDATE t_agent_run SET state = #{to}, lease_owner = #{owner}, "
            + "lease_until = now() + (#{leaseSeconds} || ' second')::interval, version = version + 1 "
            + "WHERE run_id = #{runId} AND state = #{from}")
    int casState(@Param("runId") String runId, @Param("from") RunState from,
                 @Param("to") RunState to, @Param("owner") String owner,
                 @Param("leaseSeconds") int leaseSeconds);

    /** 租约续期：长时间任务必须定期续，否则会被别的实例接管 */
    @Update("UPDATE t_agent_run SET lease_until = now() + (#{leaseSeconds} || ' second')::interval "
            + "WHERE run_id = #{runId} AND lease_owner = #{owner} AND state = 'RUNNING'")
    int renewLease(@Param("runId") String runId, @Param("owner") String owner,
                   @Param("leaseSeconds") int leaseSeconds);

    /** 抢过期任务：SKIP LOCKED 保证多实例不抢同一条 */
    @Select("UPDATE t_agent_run SET lease_owner = #{owner}, "
            + "lease_until = now() + (#{leaseSeconds} || ' second')::interval "
            + "WHERE run_id IN ("
            + "  SELECT run_id FROM t_agent_run "
            + "  WHERE state IN ('PENDING','RUNNING') AND (lease_until IS NULL OR lease_until < now()) "
            + "  ORDER BY started_at LIMIT #{batch} FOR UPDATE SKIP LOCKED"
            + ") RETURNING run_id, tenant_id, session_id, mode, query, state_json::text AS state_json, step_index")
    List<Map<String, Object>> claimExpired(@Param("owner") String owner,
                                           @Param("leaseSeconds") int leaseSeconds,
                                           @Param("batch") int batch);

    @Select("SELECT * FROM t_agent_run WHERE run_id = #{runId}")
    Map<String, Object> find(@Param("runId") String runId);

    @Update("UPDATE t_agent_run SET state = #{state}, error_code = #{code}, error_msg = #{msg}, "
            + "token_used = #{tokenUsed}, step_index = #{step}, state_json = #{stateJson}::jsonb, "
            + "finished_at = now(), lease_until = now() WHERE run_id = #{runId}")
    int finish(@Param("runId") String runId, @Param("state") RunState state,
               @Param("code") String code, @Param("msg") String msg,
               @Param("tokenUsed") int tokenUsed, @Param("step") int step,
               @Param("stateJson") String stateJson);
}
