package cn.com.agentflow.stream;

public final class EventType {
    public static final String RUN_STARTED      = "run.started";
    public static final String STEP_STARTED     = "step.started";
    public static final String LLM_DELTA        = "llm.delta";
    public static final String TOOL_CALL        = "tool.call";
    public static final String TOOL_RESULT      = "tool.result";
    public static final String WAITING_APPROVAL = "run.waiting_approval";
    public static final String RUN_SUCCEEDED    = "run.succeeded";
    public static final String RUN_FAILED       = "run.failed";
    public static final String RUN_CANCELED     = "run.canceled";
    /** 慢消费者滞后提示：有界缓冲丢最旧后补发，payload 带 lagged 与建议的 lastEventId，客户端据此重连回放 */
    public static final String LAGGED           = "stream.lagged";
    public static final String PING             = "ping";

    private EventType() {
    }
}
