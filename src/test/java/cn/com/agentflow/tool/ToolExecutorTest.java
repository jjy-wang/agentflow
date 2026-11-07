package cn.com.agentflow.tool;

import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.agent.RunLimits;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.run.ApprovalService;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.stream.SseEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** ToolExecutor：入参校验 / 高危审批拦截 / toolCallId 幂等（md 18.1） */
@ExtendWith(MockitoExtension.class)
class ToolExecutorTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Mock
    private ToolRegistry registry;
    @Mock
    private ToolInvocationStore invocationStore;
    @Mock
    private ApprovalService approvalService;

    private ToolExecutor executor;
    private RunContext ctx;

    @BeforeEach
    void setUp() {
        executor = new ToolExecutor(registry, invocationStore, approvalService, mapper);
        ctx = new RunContext("1", "run-1", 100L, "REACT", "mock-large",
                new RunLimits(12, 8, 60_000, 32_000),
                Sinks.many().multicast().onBackpressureBuffer());
    }

    private JsonNode citySchema() throws Exception {
        return mapper.readTree("{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\"}},\"required\":[\"city\"]}");
    }

    private ToolMeta lowRiskTool(AtomicInteger handlerCalls) throws Exception {
        ToolHandler handler = (args, ctx) -> {
            handlerCalls.incrementAndGet();
            return ToolResult.ok(mapper.readTree("{\"found\":true}"));
        };
        return new ToolMeta("asset_search", "查询资产", citySchema(), 1, 5000, handler);
    }

    @Test
    void 缺少必填参数返回TOOL_ARGS_INVALID且不执行handler() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        when(registry.find("asset_search")).thenReturn(Optional.of(lowRiskTool(calls)));

        ToolCall call = new ToolCall("c-1", "asset_search", "{}");
        StepVerifier.create(executor.execute(call, ctx))
                .assertNext(r -> {
                    assertFalse(r.success());
                    assertEquals("TOOL_ARGS_INVALID", r.errorCode());
                })
                .verifyComplete();
        assertEquals(0, calls.get(), "入参校验失败必须短路，handler 不得执行");
    }

    @Test
    void 未注册工具返回TOOL_NOT_FOUND() {
        when(registry.find("ghost")).thenReturn(Optional.empty());
        ToolCall call = new ToolCall("c-2", "ghost", "{}");
        StepVerifier.create(executor.execute(call, ctx))
                .expectErrorMatches(e -> e instanceof AgentException ae
                        && "TOOL_NOT_FOUND".equals(ae.getCode()))
                .verify();
    }

    @Test
    void 同一toolCallId第二次直接返回缓存结果() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        when(registry.find("asset_search")).thenReturn(Optional.of(lowRiskTool(calls)));
        // 首次插入成功，第二次插入冲突 → 走缓存
        when(invocationStore.tryStart("run-1", "c-3", "asset_search"))
                .thenReturn(Mono.just(true))
                .thenReturn(Mono.just(false));
        when(invocationStore.finish(eq("run-1"), eq("c-3"), any())).thenReturn(Mono.empty());
        ToolResult cached = ToolResult.ok(mapper.readTree("{\"found\":true}"));
        when(invocationStore.findResult("run-1", "c-3"))
                .thenReturn(Mono.just(mapper.writeValueAsString(cached)));

        ToolCall call = new ToolCall("c-3", "asset_search", "{\"city\":\"beijing\"}");
        StepVerifier.create(executor.execute(call, ctx))
                .assertNext(r -> assertTrue(r.success()))
                .verifyComplete();
        StepVerifier.create(executor.execute(call, ctx))
                .assertNext(r -> {
                    assertTrue(r.success());
                    assertTrue(r.data().get("found").asBoolean(), "第二次必须返回首次缓存的结果");
                })
                .verifyComplete();
        assertEquals(1, calls.get(), "同一 toolCallId 重复执行时 handler 只能被调用一次");
    }

    @Test
    void 高危工具返回WAITING_APPROVAL且不执行不落库() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ToolMeta meta = new ToolMeta("asset_delete", "删除资产", citySchema(), 3, 5000,
                (args, ctx) -> {
                    calls.incrementAndGet();
                    return ToolResult.ok(mapper.readTree("{}"));
                });
        when(registry.find("asset_delete")).thenReturn(Optional.of(meta));
        when(approvalService.requestApproval(any(), any(), any())).thenReturn(Mono.empty());

        ToolCall call = new ToolCall("c-4", "asset_delete", "{\"city\":\"beijing\"}");
        StepVerifier.create(executor.execute(call, ctx))
                .assertNext(r -> {
                    assertFalse(r.success());
                    assertEquals("WAITING_APPROVAL", r.errorCode());
                })
                .verifyComplete();
        assertEquals(0, calls.get(), "风险等级 >=3 的工具在审批前不得执行");
        verify(invocationStore, never()).tryStart(any(), any(), any());
    }
}
