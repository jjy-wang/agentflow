package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 租户 */
@Data
@TableName("t_tenant")
public class Tenant {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 租户编码，全局唯一 */
    private String code;

    private String name;

    /** 套餐档位：1 免费 / 2 标准 / 3 高级 */
    private Integer tier;

    /** 状态：1 正常 / 0 停用 */
    private Integer status;

    private OffsetDateTime createdAt;
}
