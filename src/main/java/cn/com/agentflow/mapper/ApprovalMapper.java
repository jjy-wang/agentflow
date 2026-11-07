package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.Approval;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 审批表基础 Mapper（唯一键 run_id + step_index） */
@Mapper
public interface ApprovalMapper extends BaseMapper<Approval> {
}
