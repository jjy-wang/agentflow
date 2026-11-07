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
 * 漏洞查询工具（只读，低风险）
 */
@Component
@RequiredArgsConstructor
public class VulnTools {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    /** keyset 分页：用 id 游标，避免深分页 offset */
    @Tool(name = "vuln_search", description = "按关键字与危险等级查询漏洞，游标分页。")
    public ToolResult search(@ToolParam(name = "keyword", description = "漏洞名称关键字", required = true) String keyword,
                             @ToolParam(name = "level", description = "危险等级 1-4") Integer level,
                             @ToolParam(name = "cursor", description = "上一页最后一条 ID") Integer cursor,
                             @ToolParam(name = "limit", description = "返回条数，1-50") Integer limit) {
        int n = limit == null ? 20 : Math.min(50, Math.max(1, limit));
        int c = cursor == null ? 0 : cursor;
        int lv = level == null ? 0 : level;

        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, asset_id, name, level FROM t_vuln "
                        + "WHERE name ILIKE '%'||?||'%' AND (? = 0 OR level = ?) AND id > ? "
                        + "ORDER BY id LIMIT ?", keyword, lv, lv, c, n);

        ObjectNode root = mapper.createObjectNode();
        root.set("items", mapper.valueToTree(rows));
        root.put("nextCursor", rows.isEmpty() ? null : ((Number) rows.get(rows.size() - 1).get("id")).longValue());
        return ToolResult.ok(root);
    }
}
