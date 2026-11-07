package cn.com.agentflow.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** SchemaGenerator：@ToolParam → JSON Schema（required / 类型映射 / 描述） */
class SchemaGeneratorTest {

    private final SchemaGenerator generator = new SchemaGenerator(new ObjectMapper());

    @SuppressWarnings("unused")
    static class SampleTools {
        public String search(
                @ToolParam(name = "keyword", description = "搜索关键词", required = true) String keyword,
                @ToolParam(name = "limit", description = "返回条数") int limit,
                @ToolParam(name = "deep", description = "是否深查") boolean deep) {
            return keyword;
        }
    }

    @Test
    void 生成完整schema_必填与类型正确() throws Exception {
        Method m = SampleTools.class.getMethod("search", String.class, int.class, boolean.class);
        JsonNode schema = generator.generate(m);

        assertEquals("object", schema.get("type").asText());
        JsonNode props = schema.get("properties");
        assertEquals("string", props.get("keyword").get("type").asText());
        assertEquals("搜索关键词", props.get("keyword").get("description").asText());
        assertEquals("integer", props.get("limit").get("type").asText());
        assertEquals(0, props.get("limit").get("minimum").asInt(), "数值参数默认带 minimum:0");
        assertEquals("boolean", props.get("deep").get("type").asText());

        JsonNode required = schema.get("required");
        assertEquals(1, required.size());
        assertEquals("keyword", required.get(0).asText(), "仅 required=true 的参数进入 required 列表");
    }

    @Test
    void 无参数方法生成空properties() throws Exception {
        Method m = getClass().getDeclaredMethod("noArgTool");
        JsonNode schema = generator.generate(m);
        assertEquals("object", schema.get("type").asText());
        assertEquals(0, schema.get("properties").size());
        assertEquals(0, schema.get("required").size());
    }

    @Tool(name = "noArgTool", description = "无参工具")
    void noArgTool() {
        // 仅用于反射取签名
    }
}
