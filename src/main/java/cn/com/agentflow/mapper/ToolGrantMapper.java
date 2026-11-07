package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.ToolGrant;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 工具授权表基础 Mapper（按租户授权工具与 RPM 上限） */
@Mapper
public interface ToolGrantMapper extends BaseMapper<ToolGrant> {
}
