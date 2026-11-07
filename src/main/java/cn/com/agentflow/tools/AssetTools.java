package cn.com.agentflow.tools;

import cn.com.agentflow.tool.Tool;
import cn.com.agentflow.tool.ToolParam;
import cn.com.agentflow.tool.ToolResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 资产查询工具（只读，低风险）
 */
@Component
@RequiredArgsConstructor
public class AssetTools {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    @Tool(name = "asset_search", description = "按关键字查询资产，最多返回 20 条。适用于“有哪些主机”“查找某 IP”这类问题。")
    public ToolResult search(@ToolParam(name = "keyword", description = "主机名或 IP 关键字", required = true) String keyword,
                             @ToolParam(name = "limit", description = "返回条数，1-20") Integer limit) {
        int n = limit == null ? 10 : Math.min(20, Math.max(1, limit));
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, ip, hostname, os FROM t_asset "
                        + "WHERE deleted = 0 AND (ip ILIKE '%'||?||'%' OR hostname ILIKE '%'||?||'%') "
                        + "ORDER BY id LIMIT ?", keyword, keyword, n);
        ObjectNode root = mapper.createObjectNode();
        root.set("items", mapper.valueToTree(rows));
        root.put("count", rows.size());
        return ToolResult.ok(root);
    }

    /** 单条 SQL 出全部统计，避免 N+1 */
    @Tool(name = "asset_detail", description = "查询单个资产的详情，包含各危险等级漏洞数量统计。")
    public ToolResult detail(@ToolParam(name = "assetId", description = "资产 ID", required = true) Integer assetId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT a.id, a.ip, a.hostname, a.os, "
                        + " count(v.*) FILTER (WHERE v.level = 4) AS critical, "
                        + " count(v.*) FILTER (WHERE v.level = 3) AS high, "
                        + " count(v.*) FILTER (WHERE v.level = 2) AS medium, "
                        + " count(v.*) FILTER (WHERE v.level = 1) AS low "
                        + "FROM t_asset a LEFT JOIN t_vuln v ON v.asset_id = a.id "
                        + "WHERE a.id = ? AND a.deleted = 0 GROUP BY a.id, a.ip, a.hostname, a.os", assetId);
        if (rows.isEmpty()) {
            return ToolResult.fail("ASSET_NOT_FOUND", "资产不存在: " + assetId);
        }
        return ToolResult.ok(mapper.valueToTree(rows.get(0)));
    }
}
