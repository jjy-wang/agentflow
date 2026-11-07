package cn.com.agentflow.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Set;

/**
 * 报表任务消息体，字段与 tools/ReportTools 投递的 JSON 严格一致：
 * {"taskId":"...","format":"pdf","assetIds":["..."]}
 */
public record ReportTaskMessage(String taskId, String format, JsonNode assetIds) {

    /** 工具声明支持的格式，非法格式属于不可重试错误 */
    private static final Set<String> SUPPORTED_FORMATS = Set.of("pdf", "docx", "xlsx");

    /** 只做结构解析，字段合法性交给 validate，便于非法参数落 FAILED 而不是丢消息 */
    public static ReportTaskMessage parse(ObjectMapper mapper, String raw) {
        if (raw == null || raw.isBlank()) {
            throw new InvalidException("消息体为空");
        }
        JsonNode root;
        try {
            root = mapper.readTree(raw);
        } catch (JsonProcessingException e) {
            throw new InvalidException("消息体不是合法 JSON", e);
        }
        if (root == null || !root.isObject()) {
            throw new InvalidException("消息体必须是 JSON 对象");
        }
        JsonNode taskId = root.get("taskId");
        JsonNode format = root.get("format");
        return new ReportTaskMessage(
                taskId == null || taskId.isNull() ? null : taskId.asText(),
                format == null || format.isNull() ? null : format.asText(),
                root.get("assetIds"));
    }

    /** 参数非法属于不可重试错误：标记 FAILED 后 ACK，避免毒消息反复投递 */
    public void validate() {
        if (taskId == null || taskId.isBlank()) {
            throw new InvalidException("taskId 不能为空");
        }
        if (format == null || !SUPPORTED_FORMATS.contains(format)) {
            throw new InvalidException("format 必须是 pdf/docx/xlsx 之一");
        }
        if (assetIds == null || !assetIds.isArray() || assetIds.isEmpty()) {
            throw new InvalidException("assetIds 必须是非空数组");
        }
        for (JsonNode assetId : assetIds) {
            if (assetId == null || !assetId.isTextual() || assetId.asText().isBlank()) {
                throw new InvalidException("assetIds 只能包含非空字符串");
            }
        }
    }

    /** 消息结构或参数非法 */
    public static class InvalidException extends RuntimeException {
        public InvalidException(String message) {
            super(message);
        }

        public InvalidException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
