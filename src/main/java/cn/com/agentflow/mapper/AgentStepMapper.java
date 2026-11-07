package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.AgentStep;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** Agent 步骤表基础 Mapper */
@Mapper
public interface AgentStepMapper extends BaseMapper<AgentStep> {
}
