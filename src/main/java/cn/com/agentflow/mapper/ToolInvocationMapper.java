package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.ToolInvocation;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 工具调用幂等表基础 Mapper（唯一键 run_id + tool_call_id） */
@Mapper
public interface ToolInvocationMapper extends BaseMapper<ToolInvocation> {
}
