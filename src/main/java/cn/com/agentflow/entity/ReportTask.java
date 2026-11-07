package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;
import java.util.UUID;

/** 报表异步任务（report_task 表无 t_ 前缀，见 V5 迁移脚本） */
@Data
@TableName("report_task")
public class ReportTask {

    @TableId
    private UUID taskId;

    /** 报表参数，JSONB 原样存取为字符串 */
    private String payload;

    /** PENDING / RUNNING / SUCCEEDED / FAILED */
    private String status;

    private OffsetDateTime createdAt;
}
