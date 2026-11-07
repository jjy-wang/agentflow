package cn.com.agentflow.telemetry;

import io.opentelemetry.api.common.AttributeKey;

import java.util.List;

/**
 * OpenTelemetry GenAI 语义约定属性名。
 * 只允许低基数维度：模型名、工具名、操作名、完成原因；
 * runId / tenantId / sessionId 一律不入 span 属性，单次 run 的关联靠 traceId。
 */
public final class GenAiAttributes {

    /** 供应商标识；本项目统一走 OpenAI 兼容协议 */
    public static final String SYSTEM = "gen_ai.system";
    public static final String SYSTEM_OPENAI = "openai";

    /** 操作类型：invoke_agent / chat / execute_tool */
    public static final String OPERATION_NAME = "gen_ai.operation.name";
    public static final String OPERATION_INVOKE_AGENT = "invoke_agent";
    public static final String OPERATION_CHAT = "chat";
    public static final String OPERATION_EXECUTE_TOOL = "execute_tool";

    public static final String REQUEST_MODEL = "gen_ai.request.model";
    public static final String REQUEST_MAX_TOKENS = "gen_ai.request.max_tokens";
    public static final String REQUEST_TEMPERATURE = "gen_ai.request.temperature";

    public static final String USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens";
    public static final String USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens";
    public static final String RESPONSE_FINISH_REASONS = "gen_ai.response.finish_reasons";
    public static final AttributeKey<List<String>> RESPONSE_FINISH_REASONS_KEY =
            AttributeKey.stringArrayKey(RESPONSE_FINISH_REASONS);

    public static final String TOOL_NAME = "gen_ai.tool.name";

    /** 默认只写长度；开启 capture-content 后才写下面两个内容属性 */
    public static final String PROMPT_LENGTH = "gen_ai.prompt.length";
    public static final String COMPLETION_LENGTH = "gen_ai.completion.length";
    public static final String PROMPT = "gen_ai.prompt";
    public static final String COMPLETION = "gen_ai.completion";

    /** 业务侧低基数补充属性 */
    public static final String RUN_MODE = "agentflow.run.mode";
    public static final String RUN_STATE = "agentflow.run.state";
    public static final String TOOL_SUCCESS = "agentflow.tool.success";
    public static final String ERROR_TYPE = "error.type";

    private GenAiAttributes() {
    }
}
