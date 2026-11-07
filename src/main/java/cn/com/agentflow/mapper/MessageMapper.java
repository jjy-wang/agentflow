package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.Message;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 会话消息表基础 Mapper */
@Mapper
public interface MessageMapper extends BaseMapper<Message> {
}
