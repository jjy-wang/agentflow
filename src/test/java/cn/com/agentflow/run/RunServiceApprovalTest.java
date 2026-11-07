package cn.com.agentflow.run;

import cn.com.agentflow.agent.AgentRuntime;
import cn.com.agentflow.agent.AgentState;
import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.mapper.RunMapper;
import cn.com.agentflow.metric.AgentMetrics;
import cn.com.agentflow.stream.RunEventBus;
import cn.com.agentflow.stream.SseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * RunService 审批续跑：从 state_json 精确恢复（含待执行工具批次）并真正跑到 SUCCEEDED；
 * 驳回/超时走终态收尾并释放 SSE 资源；CAS 抢占失败不会双跑。
 */
class RunServiceApprovalTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private RunMapper runMapper;
    private AgentRuntime agentRuntime;
    private RunEventBus eventBus;
    private AgentMetrics metrics;
    private RunService service;

    @BeforeEach
    void setUp() {
        runMapper = mock(RunMapper.class);
        agentRuntime = mock(AgentRuntime.class);
        eventBus = mock(RunEventBus.class);
        metrics = mock(AgentMetrics.class);
        service = new RunService(runMapper, agentRuntime, eventBus, metrics,
                new AgentFlowProperties(), mapper);
        when(eventBus.create(anyString())).thenReturn(Sinks.many().multicast().onBackpressureBuffer());
    }

    private ToolCall pendingCall() {
        return new ToolCall("call_risk", "asset_delete", "{\"id\":1}");
    }

    private Map<String, Object> rowWithState() {
        ToolCall call = pendingCall();
        AgentState snapshot = new AgentState("run-1", 2, 1, 100, 80, 10, 5,
                List.of(ChatMessage.user("删除资产"),
                        ChatMessage.assistant("调用工具", List.of(call))),
                List.of(call));
        Map<String, Object> row = new HashMap<>();
        row.put("run_id", "run-1");
        row.put("tenant_id", "1");
        row.put("session_id", 100L);
        row.put("mode", "REACT");
        row.put("query", "删除资产");
        row.put("step_index", 2);
        row.put("max_steps", 12);
        row.put("token_budget", 32000);
        row.put("state_json", snapshot.toJson(mapper));
        return row;
    }

    @Test
    void 审批通过后从挂起批次续跑并成功收尾() {
        when(runMapper.casState(eq("run-1"), eq(RunState.PENDING), eq(RunState.RUNNING),
                any(), anyInt())).thenReturn(1);
        when(runMapper.find("run-1")).thenReturn(rowWithState());
        // 在「调用续跑」的瞬间取样：之后终态事件会继续推高 seq，直接断言捕获到的对象会读到 6
        java.util.concurrent.atomic.AtomicLong seqAtResume = new java.util.concurrent.atomic.AtomicLong(-1);
        when(agentRuntime.executeResume(any())).thenAnswer(inv -> {
            seqAtResume.set(((RunContext) inv.getArgument(0)).lastEventSeq());
            return Mono.just("已完成");
        });

        service.resumeAfterApproval("run-1");

        ArgumentCaptor<RunContext> ctxCaptor = ArgumentCaptor.forClass(RunContext.class);
        verify(agentRuntime).executeResume(ctxCaptor.capture());
        RunContext resumed = ctxCaptor.getValue();
        assertEquals(2, resumed.getStep(), "步数必须从快照恢复");
        assertEquals(1, resumed.getToolCallCount(), "工具计数必须从快照恢复");
        assertEquals(1, resumed.getPendingToolCalls().size(), "待执行工具批次必须回灌");
        assertEquals(5, seqAtResume.get(), "续跑起点必须接续快照的事件序号，避免主键冲突丢事件");
        assertTrue(resumed.lastEventSeq() >= 5, "续跑过程中的事件序号只能单调递增");

        verify(runMapper).finish(eq("run-1"), eq(RunState.SUCCEEDED), isNull(), isNull(),
                anyInt(), anyInt(), anyString());
    }

    @Test
    void 抢占失败不重复执行() {
        when(runMapper.casState(anyString(), any(), any(), any(), anyInt())).thenReturn(0);

        service.resumeAfterApproval("run-1");

        verify(agentRuntime, never()).executeResume(any());
        verify(runMapper, never()).finish(anyString(), any(), any(), any(), anyInt(), anyInt(), anyString());
    }

    @Test
    void 驳回后关闭_SSE_并记录取消指标() {
        when(runMapper.find("run-1")).thenReturn(rowWithState());

        service.rejectAfterApproval("run-1");

        verify(eventBus).complete("run-1");
        verify(metrics).runFinished("REACT", RunState.CANCELED.name(), 0);
    }

    @Test
    void 超时后关闭_SSE_并记录超时指标() {
        when(runMapper.find("run-1")).thenReturn(rowWithState());

        service.expireAfterApproval("run-1");

        verify(eventBus).complete("run-1");
        verify(metrics).runFinished("REACT", RunState.TIMEOUT.name(), 0);
    }

    @Test
    void 重建上下文时保留历史工具结果不重跑() {
        when(runMapper.casState(anyString(), any(), any(), any(), anyInt())).thenReturn(1);
        when(runMapper.find("run-1")).thenReturn(rowWithState());
        when(agentRuntime.executeResume(any())).thenReturn(Mono.just("ok"));

        service.resumeAfterApproval("run-1");

        ArgumentCaptor<RunContext> ctxCaptor = ArgumentCaptor.forClass(RunContext.class);
        verify(agentRuntime).executeResume(ctxCaptor.capture());
        assertTrue(ctxCaptor.getValue().getMessages().stream()
                .anyMatch(m -> "assistant".equals(m.role()) && m.isAssistantWithTools()),
                "历史 assistant 工具调用必须回灌，用于定位挂起位置");
    }
}
