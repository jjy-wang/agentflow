package cn.com.agentflow.agent;

import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.stream.SseEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** AgentState：快照序列化 → 反序列化 → 回灌新上下文（崩溃恢复断点续跑的基础） */
class AgentStateTest {

    // 与 Spring Boot 默认一致：反序列化忽略未知字段（ChatMessage.isAssistantWithTools() 会被 Jackson 当作属性序列化）
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private RunContext newContext() {
        return new RunContext("1", "run-1", 100L, "REACT", "mock-large",
                new RunLimits(12, 8, 60_000, 32_000),
                Sinks.many().multicast().onBackpressureBuffer());
    }

    @Test
    void 序列化后反序列化得到等价快照() {
        RunContext ctx = newContext();
        ctx.append(ChatMessage.user("帮我查资产"));
        ctx.append(ChatMessage.assistant("好的，我来查询", null));
        ctx.append(ChatMessage.tool("call_1", "{\"total\":3}"));
        ctx.nextStep();
        ctx.incToolCall();
        ctx.addTokenUsed(88);
        ctx.addPromptTokens(120);

        AgentState state = AgentState.from(ctx);
        String json = state.toJson(mapper);
        AgentState restored = AgentState.fromJson(mapper, json);

        assertEquals(state, restored, "record 全字段相等即序列化回环无损");
        assertEquals(3, restored.messages().size());
        assertEquals("tool", restored.messages().get(2).role());
        assertEquals("call_1", restored.messages().get(2).toolCallId());
    }

    @Test
    void 回灌把历史消息恢复到新的运行上下文() {
        RunContext ctx = newContext();
        ctx.append(ChatMessage.user("第一轮"));
        ctx.append(ChatMessage.assistant("回复一", null));
        AgentState state = AgentState.from(ctx);

        RunContext fresh = newContext();
        state.restoreMessagesInto(fresh);
        assertEquals(2, fresh.getMessages().size());
        assertEquals("第一轮", fresh.getMessages().get(0).content());
    }

    @Test
    void 非法json反序列化抛业务异常() {
        assertThrows(cn.com.agentflow.exception.AgentException.class,
                () -> AgentState.fromJson(mapper, "{not-json"));
    }
}
