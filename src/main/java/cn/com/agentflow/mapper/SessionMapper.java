package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.Session;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 会话表基础 Mapper */
@Mapper
public interface SessionMapper extends BaseMapper<Session> {
}
