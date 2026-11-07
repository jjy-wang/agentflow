package cn.com.agentflow.telemetry;

import cn.com.agentflow.audit.SensitiveMasker;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 对 OTel Span 的薄包装：屏蔽无采样时的空操作，并统一 GenAI 属性写入口径。
 * 不持有线程本地状态，父子上下文由 Reactor Context 显式传递。
 */
public final class GenAiSpan {

    private static final int ERROR_MESSAGE_MAX_CHARS = 256;

    private final Span span;
    private final boolean recording;
    private final SensitiveMasker masker;
    private final boolean captureContent;
    private final int maxContentChars;
    private final Context context;
    private final AtomicBoolean ended = new AtomicBoolean(false);

    GenAiSpan(Span span, boolean recording, SensitiveMasker masker,
              boolean captureContent, int maxContentChars) {
        this.span = span;
        this.recording = recording;
        this.masker = masker;
        this.captureContent = captureContent;
        this.maxContentChars = Math.max(256, maxContentChars);
        this.context = Context.root().with(span);
    }

    static GenAiSpan noop() {
        return new GenAiSpan(Span.getInvalid(), false, null, false, 0);
    }

    public boolean isRecording() {
        return recording;
    }

    /** 创建子 span 时作为父上下文，避免依赖 ThreadLocal 的当前 span */
    Context context() {
        return context;
    }

    /** 记录请求：始终写模型、预算与 prompt 长度；内容仅在显式开启时脱敏写入 */
    public void recordRequest(ChatRequest request) {
        if (!recording || request == null) {
            return;
        }
        span.setAttribute(GenAiAttributes.REQUEST_MODEL, safe(request.model()));
        span.setAttribute(GenAiAttributes.REQUEST_MAX_TOKENS, request.maxTokens());
        span.setAttribute(GenAiAttributes.REQUEST_TEMPERATURE, request.temperature());

        int chars = 0;
        StringBuilder content = captureContent ? new StringBuilder() : null;
        List<ChatMessage> messages = request.messages();
        if (messages != null) {
            for (ChatMessage message : messages) {
                if (message.content() == null) {
                    continue;
                }
                chars += message.content().length();
                if (content != null) {
                    content.append('[').append(message.role()).append("] ")
                            .append(message.content()).append('\n');
                }
            }
        }
        span.setAttribute(GenAiAttributes.PROMPT_LENGTH, chars);
        if (content != null) {
            span.setAttribute(GenAiAttributes.PROMPT, maskAndTruncate(content.toString()));
        }
    }

    /** 记录响应：token 用量与完成原因必写，completion 内容受 capture-content 控制 */
    public void recordResponse(String completion, String finishReason,
                               int inputTokens, int outputTokens) {
        if (!recording) {
            return;
        }
        span.setAttribute(GenAiAttributes.USAGE_INPUT_TOKENS, Math.max(0, inputTokens));
        span.setAttribute(GenAiAttributes.USAGE_OUTPUT_TOKENS, Math.max(0, outputTokens));
        span.setAttribute(GenAiAttributes.RESPONSE_FINISH_REASONS_KEY,
                List.of(finishReason == null || finishReason.isBlank() ? "stop" : finishReason));
        int chars = completion == null ? 0 : completion.length();
        span.setAttribute(GenAiAttributes.COMPLETION_LENGTH, chars);
        if (captureContent && completion != null) {
            span.setAttribute(GenAiAttributes.COMPLETION, maskAndTruncate(completion));
        }
    }

    public void recordToolResult(boolean success) {
        if (recording) {
            span.setAttribute(GenAiAttributes.TOOL_SUCCESS, success);
        }
    }

    public void recordError(Throwable error) {
        if (!recording || error == null) {
            return;
        }
        String message = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
        String masked = maskAndTruncate(message);
        if (masked.length() > ERROR_MESSAGE_MAX_CHARS) {
            masked = masked.substring(0, ERROR_MESSAGE_MAX_CHARS);
        }
        span.setAttribute(GenAiAttributes.ERROR_TYPE, error.getClass().getSimpleName());
        span.setStatus(StatusCode.ERROR, masked);
    }

    /** run 根 span 收尾状态：COMPLETED / ERROR / CANCELLED */
    public void finish(String state) {
        if (recording) {
            span.setAttribute(GenAiAttributes.RUN_STATE, safe(state));
        }
    }

    /** 幂等结束：工具并行/超时取消等路径可能重复回调 */
    public void end() {
        if (ended.compareAndSet(false, true)) {
            span.end();
        }
    }

    private String maskAndTruncate(String text) {
        if (text == null) {
            return "";
        }
        String masked = masker == null ? text : masker.mask(text);
        if (masked.length() <= maxContentChars) {
            return masked;
        }
        return masked.substring(0, maxContentChars) + "...(truncated)";
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }
}
