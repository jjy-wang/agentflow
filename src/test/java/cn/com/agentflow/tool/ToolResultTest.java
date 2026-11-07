package cn.com.agentflow.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ToolResult.toModelContent：回灌给模型的内容超长时截断并附「已截断」（md 18.1） */
class ToolResultTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void 超长结果被截断且包含已截断标记() {
        ToolResult r = ToolResult.ok(mapper.valueToTree(java.util.Map.of("data", "x".repeat(9000))));
        String content = r.toModelContent(1000);
        assertTrue(content.contains("已截断"), "截断内容必须含「已截断」提示");
        // 头 500 + 标记 + 尾 500，总长远小于原文
        assertTrue(content.length() < 1200, "截断后长度必须收敛到预算附近，实际: " + content.length());
    }

    @Test
    void 短结果原样返回() {
        ToolResult r = ToolResult.ok(mapper.valueToTree(java.util.Map.of("data", "ok")));
        String content = r.toModelContent(1000);
        assertFalse(content.contains("已截断"));
        assertTrue(content.contains("ok"));
    }

    @Test
    void 失败结果序列化为错误信息() {
        ToolResult r = ToolResult.fail("DB_ERROR", "连接超时");
        String content = r.toModelContent(1000);
        assertTrue(content.contains("DB_ERROR"));
        assertTrue(content.contains("连接超时"));
    }

    @Test
    void data为null时输出空对象不报错() {
        ToolResult r = ToolResult.ok(null);
        assertEquals("{}", r.toModelContent(1000));
    }

    @Test
    void preview超过200字符截断() {
        ToolResult r = ToolResult.ok(mapper.valueToTree(java.util.Map.of("data", "y".repeat(500))));
        assertEquals(203, r.preview().length());
    }
}
