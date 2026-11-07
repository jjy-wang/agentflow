package cn.com.agentflow.telemetry;

import cn.com.agentflow.audit.SensitiveMasker;
import cn.com.agentflow.config.AgentFlowProperties;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.util.context.ContextView;

/**
 * GenAI 语义约定 span 工厂（对标 langfuse / openllmetry 的 trace 树）。
 *
 * <p>span 树：{@code invoke_agent {model}} → {@code chat {model}} → {@code execute_tool {tool}}。</p>
 *
 * <p>异步安全：根 span 由 {@code ReActLoop} 写入 Reactor Context，子 span 通过显式
 * {@code setParent(context)} 建立父子关系；不使用 ThreadLocal、不依赖当前 span，
 * 因此跨线程（boundedElastic / 并行工具）仍然正确。</p>
 *
 * <p>采样：由 {@code TracingConfig} 中的全局 OTel Sampler 统一控制
 * （{@code agentflow.telemetry.sample-rate}），这里只根据 span 是否 recording
 * 决定是否继续创建子 span，避免双重采样。</p>
 */
@Component
public class GenAiTelemetry {

    /** Reactor Context key：run 根 span，随订阅链向下游传播 */
    public static final String RUN_SPAN_KEY = "agentflow.telemetry.runSpan";

    private static final String INSTRUMENTATION_NAME = "cn.com.agentflow.genai";

    private final Tracer tracer;
    private final AgentFlowProperties props;
    private final SensitiveMasker masker;

    /**
     * 必须显式 @Autowired：本类有两个构造器，Spring 无法自动推断用哪个，
     * 会退化去调用无参构造器并抛 "No default constructor found" 导致应用启动失败。
     */
    @Autowired
    public GenAiTelemetry(ObjectProvider<Tracer> tracerProvider,
                          AgentFlowProperties props, SensitiveMasker masker) {
        this(tracerProvider.getIfAvailable(
                () -> OpenTelemetry.noop().getTracer(INSTRUMENTATION_NAME)), props, masker);
    }

    /** 测试与无 OTel 环境使用的空 Tracer */
    GenAiTelemetry(Tracer tracer, AgentFlowProperties props, SensitiveMasker masker) {
        this.tracer = tracer;
        this.props = props;
        this.masker = masker;
    }

    public static GenAiTelemetry noop() {
        return new GenAiTelemetry(OpenTelemetry.noop().getTracer(INSTRUMENTATION_NAME),
                new AgentFlowProperties(), new SensitiveMasker());
    }

    /** run 根 span：父上下文为空，单次 run 一条 trace */
    public GenAiSpan beginRun(String model, String mode) {
        Span span = tracer.spanBuilder(GenAiAttributes.OPERATION_INVOKE_AGENT + " " + safe(model))
                .setAttribute(GenAiAttributes.SYSTEM, GenAiAttributes.SYSTEM_OPENAI)
                .setAttribute(GenAiAttributes.OPERATION_NAME, GenAiAttributes.OPERATION_INVOKE_AGENT)
                .setAttribute(GenAiAttributes.REQUEST_MODEL, safe(model))
                .setAttribute(GenAiAttributes.RUN_MODE, safe(mode))
                .startSpan();
        return wrap(span);
    }

    /** 每步一次 LLM 调用；父 span 是 run 根 span */
    public GenAiSpan beginChat(GenAiSpan parent, String model) {
        if (parent != null && !parent.isRecording()) {
            return GenAiSpan.noop();
        }
        SpanBuilder builder = tracer.spanBuilder(GenAiAttributes.OPERATION_CHAT + " " + safe(model))
                .setAttribute(GenAiAttributes.SYSTEM, GenAiAttributes.SYSTEM_OPENAI)
                .setAttribute(GenAiAttributes.OPERATION_NAME, GenAiAttributes.OPERATION_CHAT)
                .setAttribute(GenAiAttributes.REQUEST_MODEL, safe(model));
        if (parent != null) {
            builder.setParent(parent.context());
        }
        return wrap(builder.startSpan());
    }

    /** 工具调用；父 span 是本步的 chat span，并行执行时各自持有显式上下文 */
    public GenAiSpan beginTool(GenAiSpan parent, String toolName) {
        if (parent != null && !parent.isRecording()) {
            return GenAiSpan.noop();
        }
        SpanBuilder builder = tracer.spanBuilder(
                        GenAiAttributes.OPERATION_EXECUTE_TOOL + " " + safe(toolName))
                .setAttribute(GenAiAttributes.OPERATION_NAME, GenAiAttributes.OPERATION_EXECUTE_TOOL)
                .setAttribute(GenAiAttributes.TOOL_NAME, safe(toolName));
        if (parent != null) {
            builder.setParent(parent.context());
        }
        return wrap(builder.startSpan());
    }

    /** 从 Reactor Context 取当前 run 根 span；不存在时返回 null，由调用方退化为独立 span */
    public static GenAiSpan currentRun(ContextView view) {
        return view.hasKey(RUN_SPAN_KEY) ? view.get(RUN_SPAN_KEY) : null;
    }

    private GenAiSpan wrap(Span span) {
        AgentFlowProperties.Telemetry config = props.getTelemetry();
        return new GenAiSpan(span, span.isRecording(), masker,
                config.isCaptureContent(), config.getMaxContentChars());
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }
}
