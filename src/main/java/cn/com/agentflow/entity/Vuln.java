package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 演示漏洞：挂在资产下，level 1-4 危险等级 */
@Data
@TableName("t_vuln")
public class Vuln {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long tenantId;

    private Long assetId;

    private String name;

    private Integer level;

    private OffsetDateTime createdAt;
}
