package cn.com.agentflow.mapper;

import cn.com.agentflow.entity.Asset;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/** 资产表基础 Mapper */
@Mapper
public interface AssetMapper extends BaseMapper<Asset> {
}
