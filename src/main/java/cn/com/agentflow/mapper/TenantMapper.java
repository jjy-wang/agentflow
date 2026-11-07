package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.Tenant;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 租户表基础 Mapper */
@Mapper
public interface TenantMapper extends BaseMapper<Tenant> {
}
