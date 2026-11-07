package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/**
 * Agent 执行记录（t_agent_run）。
 * 状态机字段：state + version（乐观锁）+ leaseOwner/leaseUntil（多实例租约）。
 */
@Data
@TableName("t_agent_run")
public class AgentRunRecord {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 对外 run 标识（24 位 hex） */
    private String runId;

    private Long tenantId;

    private Long sessionId;

    /** REACT / PLAN_EXECUTE */
    private String mode;

    /** RunState 枚举名 */
    private String state;

    private String query;

    private Integer stepIndex;

    private Integer maxSteps;

    private Integer tokenBudget;

    private Integer tokenUsed;

    /** AgentState 快照 JSONB，断点续跑的数据源 */
    private String stateJson;

    /** 乐观锁版本，casState 更新时自增 */
    private Integer version;

    private String leaseOwner;

    private OffsetDateTime leaseUntil;

    private String errorCode;

    private String errorMsg;

    private OffsetDateTime startedAt;

    private OffsetDateTime finishedAt;
}
