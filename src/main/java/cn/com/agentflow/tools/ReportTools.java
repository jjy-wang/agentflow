package cn.com.agentflow.tools;

import cn.com.agentflow.config.RabbitConfig;
import cn.com.agentflow.tool.Tool;
import cn.com.agentflow.tool.ToolParam;
import cn.com.agentflow.tool.ToolResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * 报表生成工具（中风险，异步执行）
 */
@Component
@RequiredArgsConstructor
public class ReportTools {

    private final JdbcTemplate jdbc;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper mapper;

    @Tool(name = "report_generate", description = "为指定资产范围生成安全报表，异步执行，返回任务 ID。",
            riskLevel = 2, timeoutMs = 8000)
    public ToolResult generate(@ToolParam(name = "assetIds", description = "资产 ID 列表", required = true) JsonNode assetIds,
                               @ToolParam(name = "format", description = "报表格式 pdf/docx/xlsx", required = true) String format) {
        if (!assetIds.isArray() || assetIds.isEmpty()) {
            return ToolResult.fail("ARGS_INVALID", "assetIds 不能为空");
        }
        String taskId = UUID.randomUUID().toString().replace("-", "");
        ObjectNode payload = mapper.createObjectNode();
        payload.put("taskId", taskId);
        payload.put("format", format);
        payload.set("assetIds", assetIds);

        // 先落库再发消息：消息丢了还能靠扫描补发；表名以 V5 迁移为准（report_task 无 t_ 前缀）
        jdbc.update("INSERT INTO report_task(task_id, payload, status) VALUES (?,?::jsonb,'PENDING')",
                taskId, payload.toString());
        rabbitTemplate.convertAndSend(RabbitConfig.REPORT_EXCHANGE, RabbitConfig.REPORT_ROUTING_KEY, payload.toString());

        ObjectNode out = mapper.createObjectNode();
        out.put("taskId", taskId);
        out.put("status", "PENDING");
        return ToolResult.ok(out);
    }
}
