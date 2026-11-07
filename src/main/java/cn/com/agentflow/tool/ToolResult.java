package cn.com.agentflow.tool;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 工具执行结果。preview 必须是截断后的安全文本，会进入 SSE 与日志。
 * 三态语义：SUCCESS / FAILURE / WAITING_APPROVAL。
 * 待审批不是失败：ReActLoop 据此挂起 run，而不是走 TOOL_ALL_FAILED 把 run 判死。
 */
public record ToolResult(boolean success, JsonNode data, String errorCode, String errorMessage,
                         Status status) {

    /** 结果状态：成功 / 失败 / 待人工审批 */
    public enum Status {
        SUCCESS, FAILURE, WAITING_APPROVAL
    }

    /** 历史落库数据可能没有 status 字段，按 success 推导，保证反序列化后语义稳定 */
    public ToolResult {
        if (status == null) {
            status = success ? Status.SUCCESS : Status.FAILURE;
        }
    }

    public static ToolResult ok(JsonNode data) {
        return new ToolResult(true, data, null, null, Status.SUCCESS);
    }

    public static ToolResult fail(String code, String message) {
        return new ToolResult(false, null, code, message, Status.FAILURE);
    }

    /** 待审批：既不是成功也不是失败，调用方必须挂起而不是判失败 */
    public static ToolResult approvalRequired(String toolName) {
        return new ToolResult(false, null, "WAITING_APPROVAL",
                "工具 " + toolName + " 需人工审批", Status.WAITING_APPROVAL);
    }

    public boolean approvalRequired() {
        return status == Status.WAITING_APPROVAL;
    }

    /** 用于 SSE / 回灌给模型的内容，超长截断 */
    public String toModelContent(int maxChars) {
        String raw = success
                ? (data == null ? "{}" : data.toString())
                : ("{\"error\":\"" + errorCode + "\",\"message\":\"" + safe(errorMessage) + "\"}");
        if (raw.length() <= maxChars) {
            return raw;
        }
        return raw.substring(0, maxChars / 2)
                + "...[已截断 " + (raw.length() - maxChars) + " 字符]..."
                + raw.substring(raw.length() - maxChars / 2);
    }

    public String preview() {
        String raw = success ? (data == null ? "{}" : data.toString()) : (errorCode + ":" + errorMessage);
        return raw.length() > 200 ? raw.substring(0, 200) + "..." : raw;
    }

    private String safe(String s) {
        return s == null ? "" : s.replace("\"", "'");
    }
}
