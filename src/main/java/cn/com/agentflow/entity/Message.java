package cn.com.agentflow.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.OffsetDateTime;

/** 会话消息：user/assistant/tool 三种角色，assistant 可带工具调用（tool_calls JSONB） */
@Data
@TableName("t_message")
public class Message {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long sessionId;

    private Long tenantId;

    private String role;

    private String content;

    /** assistant 消息的工具调用，JSONB 原样存取为字符串 */
    private String toolCalls;

    /** tool 角色消息对应的工具调用 id */
    private String toolCallId;

    private Integer tokenIn;

    private Integer tokenOut;

    private OffsetDateTime createdAt;
}
