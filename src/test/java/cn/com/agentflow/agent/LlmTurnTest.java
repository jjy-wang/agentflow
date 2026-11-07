package cn.com.agentflow.agent;

import cn.com.agentflow.llm.ChatChunk;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LlmTurn 分片聚合：工具参数按 index 顺序拼接，usage 缺失时用量为 0（md 18.1） */
class LlmTurnTest {

    @Test
    void 工具参数分片按序拼接() {
        LlmTurn turn = LlmTurn.merge(List.of(
                new ChatChunk.TextDelta("好的"),
                new ChatChunk.ToolCallDelta(0, "call_1", "asset_search", "{\"query\":\"ab"),
                new ChatChunk.ToolCallDelta(0, null, null, "\":"),
                new ChatChunk.ToolCallDelta(0, null, null, "1}"),
                new ChatChunk.Usage(120, 30),
                new ChatChunk.Finished("tool_calls")));

        assertTrue(turn.hasToolCalls());
        assertEquals(1, turn.toolCalls().size());
        assertEquals("call_1", turn.toolCalls().get(0).id());
        assertEquals("asset_search", turn.toolCalls().get(0).name());
        assertEquals("{\"query\":\"ab\":1}", turn.toolCalls().get(0).argumentsJson(),
                "三段参数分片必须按序拼接成完整 JSON");
        assertEquals("好的", turn.text());
        assertEquals(120, turn.promptTokens());
        assertEquals(30, turn.completionTokens());
        assertEquals("tool_calls", turn.finishReason());
    }

    @Test
    void 无usage包时prompt与completion为0() {
        LlmTurn turn = LlmTurn.merge(List.of(
                new ChatChunk.TextDelta("回答"),
                new ChatChunk.Finished("stop")));
        assertEquals(0, turn.promptTokens());
        assertEquals(0, turn.completionTokens());
        assertEquals("stop", turn.finishReason());
        assertFalse(turn.hasToolCalls());
    }

    @Test
    void 空参数工具调用补空对象() {
        LlmTurn turn = LlmTurn.merge(List.of(
                new ChatChunk.ToolCallDelta(0, "call_9", "report_list", null)));
        assertEquals("{}", turn.toolCalls().get(0).argumentsJson());
    }

    @Test
    void 多个工具调用按index排序输出() {
        LlmTurn turn = LlmTurn.merge(List.of(
                new ChatChunk.ToolCallDelta(1, "call_b", "tool_b", "{}"),
                new ChatChunk.ToolCallDelta(0, "call_a", "tool_a", "{}")));
        assertEquals("call_a", turn.toolCalls().get(0).id());
        assertEquals("call_b", turn.toolCalls().get(1).id());
    }
}
