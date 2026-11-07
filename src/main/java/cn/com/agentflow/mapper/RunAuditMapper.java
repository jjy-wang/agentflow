package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.RunAudit;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 审计表基础 Mapper（链式哈希写入见 audit/RunAuditService） */
@Mapper
public interface RunAuditMapper extends BaseMapper<RunAudit> {
}
