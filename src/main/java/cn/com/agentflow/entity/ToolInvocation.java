package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 工具调用幂等表：(run_id, tool_call_id) 唯一，重复调用直接回放缓存结果 */
@Data
@TableName("t_tool_invocation")
public class ToolInvocation {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String runId;

    private String toolCallId;

    private String toolName;

    /** RUNNING / SUCCEEDED / FAILED */
    private String status;

    private String resultJson;

    private OffsetDateTime createdAt;
}
