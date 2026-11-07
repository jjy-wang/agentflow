package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 审计日志：链式哈希（row_hash 含 prev_hash），篡改任一行都会断链 */
@Data
@TableName("t_run_audit")
public class RunAudit {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String traceId;

    private Long tenantId;

    private String runId;

    /** 动作：RUN_CREATED / TOOL_EXECUTED / APPROVED ... */
    private String action;

    private String detail;

    /** 同一 run 上一条审计的 row_hash，首条为 null */
    private String prevHash;

    /** sha256(traceId|tenantId|runId|action|detail|prevHash|createdAt) */
    private String rowHash;

    private OffsetDateTime createdAt;
}
