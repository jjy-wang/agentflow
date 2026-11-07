package cn.com.agentflow.telemetry;

import cn.com.agentflow.audit.SensitiveMasker;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.ToolDef;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** GenAI 语义约定：span 树、属性、PII 默认关闭、采样短路 */
class GenAiTelemetryTest {

    private final List<SpanData> exported = new CopyOnWriteArrayList<>();
    private final AgentFlowProperties props = new AgentFlowProperties();

    @Test
    void run下生成chat与executeTool子span并写GenAi属性() {
        GenAiTelemetry telemetry = newTelemetry(Sampler.alwaysOn());

        GenAiSpan run = telemetry.beginRun("mock-large", "REACT");
        GenAiSpan chat = telemetry.beginChat(run, "mock-large");
        ChatRequest request = new ChatRequest("mock-large",
                List.of(ChatMessage.system("你是资产助手"),
                        ChatMessage.user("手机号 13812345678，查询生产库资产")),
                List.of(new ToolDef("asset_search", "查询资产", null)), 256, 0.3);
        chat.recordRequest(request);
        chat.recordResponse("已找到 2 条资产", "tool_calls", 120, 30);

        GenAiSpan tool = telemetry.beginTool(chat, "asset_search");
        tool.recordToolResult(true);
        tool.end();
        chat.end();
        run.finish("COMPLETED");
        run.end();

        assertEquals(3, exported.size());
        SpanData runData = byName("invoke_agent mock-large");
        SpanData chatData = byName("chat mock-large");
        SpanData toolData = byName("execute_tool asset_search");

        assertEquals(runData.getSpanContext(), chatData.getParentSpanContext(), "chat 必须是 run 的子 span");
        assertEquals(chatData.getSpanContext(), toolData.getParentSpanContext(), "工具必须是本步 chat 的子 span");

        Attributes runAttrs = runData.getAttributes();
        assertEquals("invoke_agent", runAttrs.get(AttributeKey.stringKey("gen_ai.operation.name")));
        assertEquals("openai", runAttrs.get(AttributeKey.stringKey("gen_ai.system")));
        assertEquals("mock-large", runAttrs.get(AttributeKey.stringKey("gen_ai.request.model")));
        assertEquals("COMPLETED", runAttrs.get(AttributeKey.stringKey("agentflow.run.state")));

        Attributes chatAttrs = chatData.getAttributes();
        assertEquals("chat", chatAttrs.get(AttributeKey.stringKey("gen_ai.operation.name")));
        assertEquals(120L, chatAttrs.get(AttributeKey.longKey("gen_ai.usage.input_tokens")));
        assertEquals(30L, chatAttrs.get(AttributeKey.longKey("gen_ai.usage.output_tokens")));
        assertEquals(List.of("tool_calls"),
                chatAttrs.get(AttributeKey.stringArrayKey("gen_ai.response.finish_reasons")));
        assertTrue(chatAttrs.get(AttributeKey.longKey("gen_ai.prompt.length")) > 0);
        assertEquals(9L, chatAttrs.get(AttributeKey.longKey("gen_ai.completion.length")));

        Attributes toolAttrs = toolData.getAttributes();
        assertEquals("execute_tool", toolAttrs.get(AttributeKey.stringKey("gen_ai.operation.name")));
        assertEquals("asset_search", toolAttrs.get(AttributeKey.stringKey("gen_ai.tool.name")));
        assertEquals(Boolean.TRUE, toolAttrs.get(AttributeKey.booleanKey("agentflow.tool.success")));
    }

    @Test
    void 默认不写prompt与completion内容() {
        GenAiTelemetry telemetry = newTelemetry(Sampler.alwaysOn());
        GenAiSpan run = telemetry.beginRun("mock-large", "REACT");
        GenAiSpan chat = telemetry.beginChat(run, "mock-large");

        chat.recordRequest(new ChatRequest("mock-large",
                List.of(ChatMessage.user("手机号 13812345678")), List.of(), 256, 0.3));
        chat.recordResponse("已脱敏", "stop", 10, 3);
        chat.end();
        run.end();

        Attributes attrs = byName("chat mock-large").getAttributes();
        assertNull(attrs.get(AttributeKey.stringKey("gen_ai.prompt")));
        assertNull(attrs.get(AttributeKey.stringKey("gen_ai.completion")));
        assertNotNull(attrs.get(AttributeKey.longKey("gen_ai.prompt.length")));
    }

    @Test
    void 开启内容采集后先脱敏再写入span() {
        props.getTelemetry().setCaptureContent(true);
        GenAiTelemetry telemetry = newTelemetry(Sampler.alwaysOn());
        GenAiSpan run = telemetry.beginRun("mock-large", "REACT");
        GenAiSpan chat = telemetry.beginChat(run, "mock-large");

        chat.recordRequest(new ChatRequest("mock-large",
                List.of(ChatMessage.user("联系 13812345678 查询")), List.of(), 256, 0.3));
        chat.recordResponse("已通知 13812345678", "stop", 10, 3);
        chat.end();
        run.end();

        Attributes attrs = byName("chat mock-large").getAttributes();
        String prompt = attrs.get(AttributeKey.stringKey("gen_ai.prompt"));
        String completion = attrs.get(AttributeKey.stringKey("gen_ai.completion"));
        assertTrue(prompt.contains("138****5678"));
        assertFalse(prompt.contains("13812345678"));
        assertTrue(completion.contains("138****5678"));
        assertFalse(completion.contains("13812345678"));
    }

    @Test
    void 未采样时不创建子span也不导出() {
        GenAiTelemetry telemetry = newTelemetry(Sampler.alwaysOff());
        GenAiSpan run = telemetry.beginRun("mock-large", "REACT");
        GenAiSpan chat = telemetry.beginChat(run, "mock-large");
        GenAiSpan tool = telemetry.beginTool(chat, "asset_search");

        assertFalse(run.isRecording());
        assertFalse(chat.isRecording());
        assertFalse(tool.isRecording());

        tool.end();
        chat.end();
        run.end();
        assertTrue(exported.isEmpty());
    }

    /**
     * 回归保护：本类有两个构造器，若漏掉 @Autowired，Spring 会去找无参构造器并抛
     * "No default constructor found"，应用直接起不来。单测只 new 对象发现不了，
     * 必须让容器真的装配一次。
     */
    @Test
    void Spring容器能装配该Bean() {
        new ApplicationContextRunner()
                .withUserConfiguration(GenAiTelemetry.class)
                .withBean(AgentFlowProperties.class, AgentFlowProperties::new)
                .withBean(SensitiveMasker.class, SensitiveMasker::new)
                .run(ctx -> {
                    assertNull(ctx.getStartupFailure(),
                            "容器装配失败: " + ctx.getStartupFailure());
                    assertNotNull(ctx.getBean(GenAiTelemetry.class));
                });
    }

    private GenAiTelemetry newTelemetry(Sampler sampler) {
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .setSampler(sampler)
                .addSpanProcessor(SimpleSpanProcessor.create(new SpanExporter() {
                    @Override
                    public CompletableResultCode export(Collection<SpanData> spans) {
                        exported.addAll(spans);
                        return CompletableResultCode.ofSuccess();
                    }

                    @Override
                    public CompletableResultCode flush() {
                        return CompletableResultCode.ofSuccess();
                    }

                    @Override
                    public CompletableResultCode shutdown() {
                        return CompletableResultCode.ofSuccess();
                    }
                }))
                .build();
        return new GenAiTelemetry(provider.get("test"), props, new SensitiveMasker());
    }

    private SpanData byName(String name) {
        return exported.stream()
                .filter(span -> name.equals(span.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("未找到 span: " + name + "，实际: "
                        + exported.stream().map(SpanData::getName).toList()));
    }
}
