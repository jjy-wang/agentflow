package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.Memory;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 长期记忆表基础 Mapper（向量列读写走 memory/VectorMemory） */
@Mapper
public interface MemoryMapper extends BaseMapper<Memory> {
}
