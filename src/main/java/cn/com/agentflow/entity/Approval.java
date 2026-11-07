package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 人工审批单：高风险工具调用挂起 run 后落库，(run_id, step_index) 唯一 */
@Data
@TableName("t_approval")
public class Approval {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String runId;

    private Integer stepIndex;

    private String toolName;

    private String argsJson;

    /** PENDING / APPROVED / REJECTED */
    private String status;

    private String approver;

    private OffsetDateTime createdAt;

    private OffsetDateTime decidedAt;
}
