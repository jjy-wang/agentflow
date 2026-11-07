package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.ToolEntity;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 工具定义表（t_tool）基础 Mapper */
@Mapper
public interface ToolEntityMapper extends BaseMapper<ToolEntity> {
}
