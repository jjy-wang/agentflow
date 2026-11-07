package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDate;

/** 配额日用量：(tenant_id, stat_day) 唯一，按租户套餐限额对账 */
@Data
@TableName("t_quota_usage")
public class QuotaUsage {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long tenantId;

    private LocalDate statDay;

    private Integer usedRuns;

    private Long usedTokens;
}
