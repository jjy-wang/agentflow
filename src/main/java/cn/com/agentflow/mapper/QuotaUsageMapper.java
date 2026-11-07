package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.QuotaUsage;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 配额日用量表基础 Mapper（唯一键 tenant_id + stat_day） */
@Mapper
public interface QuotaUsageMapper extends BaseMapper<QuotaUsage> {
}
