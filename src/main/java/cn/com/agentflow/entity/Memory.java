package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/**
 * 长期记忆（pgvector）。
 * embedding 为 vector(1024) 类型，MyBatis-Plus 不直接映射，
 * 向量的写入与召回统一走 memory/VectorMemory（JdbcTemplate + ::vector 转换）。
 */
@Data
@TableName("t_memory")
public class Memory {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long tenantId;

    private Long sessionId;

    private String content;

    /** 内容 SHA-256，(tenant_id, content_hash) 唯一去重 */
    private String contentHash;

    /** 向量列不由 MP 管理，见类注释 */
    @TableField(exist = false)
    private String embedding;

    /** 记忆权重，召回得分 = 相似度 * weight * 时效衰减 */
    private Float weight;

    private OffsetDateTime createdAt;
}
