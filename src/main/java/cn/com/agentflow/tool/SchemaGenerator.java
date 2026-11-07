package cn.com.agentflow.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 反射读取 @ToolParam，生成工具入参的 JSON Schema
 */
public class SchemaGenerator {

    private final ObjectMapper mapper;

    public SchemaGenerator(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode generate(Method method) {
        ObjectNode schema = mapper.createObjectNode();
        schema.put("type", "object");
        ObjectNode props = schema.putObject("properties");
        ArrayNode required = schema.putArray("required");

        for (Parameter p : method.getParameters()) {
            ToolParam tp = p.getAnnotation(ToolParam.class);
            if (tp == null) {
                continue;
            }
            ObjectNode field = props.putObject(tp.name());
            field.put("type", jsonType(p.getType()));
            field.put("description", tp.description());
            if (p.getType() == Integer.class || p.getType() == int.class) {
                field.put("minimum", 0);
            }
            if (tp.required()) {
                required.add(tp.name());
            }
        }
        return schema;
    }

    private String jsonType(Class<?> type) {
        if (type == int.class || type == Integer.class || type == long.class || type == Long.class) {
            return "integer";
        }
        if (type == boolean.class || type == Boolean.class) {
            return "boolean";
        }
        if (type == double.class || type == Double.class || type == float.class) {
            return "number";
        }
        return "string";
    }
}
