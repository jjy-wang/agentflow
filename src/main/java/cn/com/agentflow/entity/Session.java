package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 会话：一个租户下的多轮对话容器，summary 是压缩后的长期上下文 */
@Data
@TableName("t_session")
public class Session {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long tenantId;

    /** 业务侧会话标识，与 tenantId 联合唯一 */
    private String sessionKey;

    private String title;

    /** 已压缩的早期对话摘要 */
    private String summary;

    /** 摘要覆盖到的步数，增量压缩从这里续 */
    private Integer summaryUptoStep;

    private OffsetDateTime createdAt;

    private OffsetDateTime updatedAt;
}
