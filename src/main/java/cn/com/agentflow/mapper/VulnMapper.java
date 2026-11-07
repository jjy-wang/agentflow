package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.Vuln;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 漏洞表基础 Mapper */
@Mapper
public interface VulnMapper extends BaseMapper<Vuln> {
}
