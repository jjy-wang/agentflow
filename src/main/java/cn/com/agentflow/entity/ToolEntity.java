package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

/** 工具元数据表（类名加 Entity 后缀避免与 tool.Tool 注解混淆） */
@Data
@TableName("t_tool")
public class ToolEntity {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String name;

    private String description;

    /** JSON Schema，JSONB 原样存取为字符串 */
    private String inputSchema;

    /** 风险等级：1 低 / 2 中 / 3 高（3 必须人工审批） */
    private Integer riskLevel;

    private Integer timeoutMs;

    private Integer enabled;
}
