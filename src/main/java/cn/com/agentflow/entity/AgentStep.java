package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 执行步骤明细：LLM 轮次与工具调用的审计级记录 */
@Data
@TableName("t_agent_step")
public class AgentStep {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String runId;

    private Integer stepIndex;

    /** LLM / TOOL */
    private String kind;

    private String toolName;

    private String inputJson;

    private String outputJson;

    private Integer costMs;

    /** 0 成功，非 0 为错误码 */
    private Integer code;

    private OffsetDateTime createdAt;
}
