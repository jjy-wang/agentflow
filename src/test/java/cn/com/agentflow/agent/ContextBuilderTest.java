package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.ToolDef;
import cn.com.agentflow.prompts.PromptAssembler;
import cn.com.agentflow.prompts.PromptPrefixMeter;
import cn.com.agentflow.stream.SseEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ContextBuilder：历史整轮裁剪 + 工具 schema 按预算丢弃（md 18.1）+ prompt 前缀稳定性 */
class ContextBuilderTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final ContextBuilder builder = new ContextBuilder(new AgentFlowProperties(),
            new PromptPrefixMeter(meterRegistry));

    private RunContext newContext(int tokenBudget) {
        return new RunContext("1", "run-1", 100L, "REACT", "mock-large",
                new RunLimits(12, 8, 60_000, tokenBudget),
                Sinks.many().multicast().onBackpressureBuffer());
    }

    @Test
    void 历史超预算时整轮裁剪且首条是user() {
        RunContext ctx = newContext(1000);
        // 单条成本 = estimate(60 个中文字) + 8 = 48，一轮 96；6 轮共 576 > 历史预算 450
        String content = "数据".repeat(30);
        for (int i = 0; i < 6; i++) {
            ctx.append(ChatMessage.user("第" + i + "问 " + content));
            ctx.append(ChatMessage.assistant("第" + i + "答 " + content, null));
        }
        ChatRequest req = builder.build(ctx, List.of(), "系统提示");
        // messages[0] 是 system，其后为保留下来的历史
        int keptHistory = req.messages().size() - 1;
        assertTrue(keptHistory < 12, "超预算必须裁掉部分历史，实际保留: " + keptHistory);
        assertTrue(keptHistory > 0);
        assertEquals("system", req.messages().get(0).role());
        assertEquals("user", req.messages().get(1).role(), "裁剪按轮进行，保留历史的首条必须是 user");
        assertEquals(0, keptHistory % 2, "整轮裁剪：保留的消息数必须为偶数（user+assistant 成对）");
    }

    @Test
    void 历史未超预算时全量保留() {
        RunContext ctx = newContext(32_000);
        ctx.append(ChatMessage.user("你好"));
        ctx.append(ChatMessage.assistant("你好，请问需要什么帮助？", null));
        ChatRequest req = builder.build(ctx, List.of(), "系统提示");
        assertEquals(3, req.messages().size());
    }

    @Test
    void 工具schema超预算时丢弃排后的工具() {
        RunContext ctx = newContext(1000);
        // 工具预算 = 1000 * 0.15 = 150；每个工具成本 = estimate(desc) + 40 = 90
        String desc = "x".repeat(200);
        List<ToolDef> tools = List.of(
                new ToolDef("tool_a", desc, null),
                new ToolDef("tool_b", desc, null));
        ChatRequest req = builder.build(ctx, tools, "系统提示");
        assertEquals(1, req.tools().size(), "预算只能容纳一个工具");
        assertEquals("tool_a", req.tools().get(0).name(), "按字典序稳定保留，排后的被丢弃");
    }

    @Test
    void 输出预算按配置比例下拨() {
        RunContext ctx = newContext(1000);
        ChatRequest req = builder.build(ctx, List.of(), "系统提示");
        assertEquals((int) (1000 * 0.30), req.maxTokens());
    }

    @Test
    void 同一工具集两次构建前缀指纹一致且复用指标递增() {
        RunContext ctx = newContext(32_000);
        List<ToolDef> tools = List.of(
                new ToolDef("asset_search", "查询资产", null),
                new ToolDef("database_query", "查询数据库", null));

        ChatRequest first = builder.build(ctx, tools, "固定角色说明");
        ChatRequest second = builder.build(ctx, tools, "固定角色说明");

        String firstFingerprint = PromptAssembler.fingerprint("固定角色说明", first.tools());
        String secondFingerprint = PromptAssembler.fingerprint("固定角色说明", second.tools());
        assertEquals(firstFingerprint, secondFingerprint);
        assertEquals(16, firstFingerprint.length(), "指纹取 sha256 前 16 位");
        assertEquals(2.0, meterRegistry.get(PromptPrefixMeter.BUILD_METRIC).counter().count());
        assertEquals(1.0, meterRegistry.get(PromptPrefixMeter.REUSED_METRIC).counter().count(),
                "第二次出现同一指纹即计一次复用");
    }

    @Test
    void 记忆注入变化不影响稳定前缀指纹() {
        List<ToolDef> tools = List.of(new ToolDef("asset_search", "查询资产", null));
        String stable = "固定角色说明";
        String withMemory = stable + "\n\n【长期记忆·自动召回】\n- 上次提到的生产库位于机房 A";

        assertEquals(PromptAssembler.stablePrefix(stable), PromptAssembler.stablePrefix(withMemory));
        assertEquals(PromptAssembler.fingerprint(stable, tools),
                PromptAssembler.fingerprint(withMemory, tools));
        assertNotEquals(PromptAssembler.fingerprint(stable, tools),
                PromptAssembler.fingerprint("固定角色说明（变更）", tools));
    }

    @Test
    void 打乱工具注册顺序后输出顺序与指纹一致() {
        List<ToolDef> ordered = List.of(
                new ToolDef("asset_search", "查询资产", null),
                new ToolDef("database_query", "查询数据库", null),
                new ToolDef("log_query", "查询日志", null));
        List<ToolDef> shuffled = List.of(ordered.get(2), ordered.get(0), ordered.get(1));

        ChatRequest a = builder.build(newContext(32_000), ordered, "固定角色说明");
        ChatRequest b = builder.build(newContext(32_000), shuffled, "固定角色说明");

        assertEquals(List.of("asset_search", "database_query", "log_query"),
                a.tools().stream().map(ToolDef::name).toList());
        assertEquals(a.tools().stream().map(ToolDef::name).toList(),
                b.tools().stream().map(ToolDef::name).toList());
        assertEquals(PromptAssembler.fingerprint("固定角色说明", a.tools()),
                PromptAssembler.fingerprint("固定角色说明", b.tools()));
    }
}
