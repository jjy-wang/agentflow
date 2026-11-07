package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** API Key：库里只存 SHA-256 哈希与前缀，明文不落库 */
@Data
@TableName("t_api_key")
public class ApiKey {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long tenantId;

    /** 明文前 12 位，用于展示与快速定位 */
    private String keyPrefix;

    /** 明文 key 的 SHA-256（hex） */
    private String keyHash;

    private Integer status;

    private OffsetDateTime expireAt;

    private OffsetDateTime createdAt;
}
