package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.ApiKey;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** API Key 表基础 Mapper（高频校验走 ApiKeyService + 本地缓存） */
@Mapper
public interface ApiKeyMapper extends BaseMapper<ApiKey> {
}
