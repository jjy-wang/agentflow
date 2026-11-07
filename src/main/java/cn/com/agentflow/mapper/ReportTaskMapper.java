package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.ReportTask;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 报表异步任务表基础 Mapper（表名 report_task，无 t_ 前缀） */
@Mapper
public interface ReportTaskMapper extends BaseMapper<ReportTask> {
}
