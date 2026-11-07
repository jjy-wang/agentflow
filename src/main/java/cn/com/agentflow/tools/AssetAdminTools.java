package cn.com.agentflow.tools;

import cn.com.agentflow.tool.Tool;
import cn.com.agentflow.tool.ToolParam;
import cn.com.agentflow.tool.ToolResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 资产管理工具（高风险，必须人工审批）
 */
@Component
@RequiredArgsConstructor
public class AssetAdminTools {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    /** 高风险：必须人工审批后才真正执行 */
    @Tool(name = "asset_delete", description = "删除资产（逻辑删除）。属于高风险操作，必须人工审批。",
            riskLevel = 3)
    public ToolResult delete(@ToolParam(name = "assetId", description = "资产 ID", required = true) Integer assetId) {
        int n = jdbc.update("UPDATE t_asset SET deleted = 1 WHERE id = ? AND deleted = 0", assetId);
        ObjectNode out = mapper.createObjectNode();
        out.put("assetId", assetId);
        out.put("deleted", n > 0);
        return ToolResult.ok(out);
    }
}
