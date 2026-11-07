package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

/** 租户工具授权：白名单 + 每租户每工具 RPM 上限 */
@Data
@TableName("t_tool_grant")
public class ToolGrant {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long tenantId;

    private String toolName;

    private Integer rpmLimit;

    private Integer enabled;
}
