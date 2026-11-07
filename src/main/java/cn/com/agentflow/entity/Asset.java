package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 演示资产：供 asset_search / asset_detail / asset_delete 工具查询 */
@Data
@TableName("t_asset")
public class Asset {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long tenantId;

    private String ip;

    private String hostname;

    private String os;

    /** 逻辑删除标记：1 已删除 */
    private Integer deleted;

    private OffsetDateTime createdAt;
}
