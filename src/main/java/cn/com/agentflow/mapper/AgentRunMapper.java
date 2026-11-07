package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.AgentRunRecord;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** t_agent_run 实体 Mapper：状态机字段操作见 RunMapper（注解 SQL） */
@Mapper
public interface AgentRunMapper extends BaseMapper<AgentRunRecord> {
}
