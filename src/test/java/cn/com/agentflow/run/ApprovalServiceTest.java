package cn.com.agentflow.run;

import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.agent.RunLimits;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.stream.EventType;
import cn.com.agentflow.stream.SseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 审批服务：挂起落库/事件上下文、通过后立即续跑、驳回释放资源、
 * 重复审批 CAS 只生效一次、跨租户拒绝、超时收敛。
 */
class ApprovalServiceTest {

    private JdbcTemplate jdbc;
    private RunService runService;
    private ObjectProvider<RunService> runServiceProvider;
    private ApprovalService service;
    private Sinks.Many<SseEvent> sink;
    private List<SseEvent> events;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        runService = mock(RunService.class);
        runServiceProvider = mock(ObjectProvider.class);
        when(runServiceProvider.getObject()).thenReturn(runService);

        service = new ApprovalService(jdbc, new ObjectMapper(), new AgentFlowProperties(), runServiceProvider);

        sink = Sinks.many().multicast().onBackpressureBuffer();
        events = new CopyOnWriteArrayList<>();
        sink.asFlux().subscribe(events::add);
    }

    private RunContext newContext() {
        RunContext ctx = new RunContext("1", "run-1", 100L, "REACT", "mock-large",
                new RunLimits(12, 20, 60_000, 32_000), sink);
        ctx.append(ChatMessage.user("删除资产"));
        ctx.nextStep();
        ctx.suspend(List.of(new ToolCall("call_risk", "asset_delete", "{\"id\":1}")));
        return ctx;
    }

    @Test
    void 挂起时持久化待执行批次并发出带上下文的审批事件() {
        when(jdbc.queryForObject(contains("RETURNING id"), eq(Long.class), any(Object[].class)))
                .thenReturn(88L);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);

        RunContext ctx = newContext();
        assertEquals(88L, service.requestApproval(ctx, ctx.getPendingToolCalls().get(0), null).block());
        service.suspend(ctx, 88L).block();

        ArgumentCaptor<Object[]> args = ArgumentCaptor.forClass(Object[].class);
        verify(jdbc).update(contains("state='WAITING_APPROVAL'"), args.capture());
        String stateJson = String.valueOf(args.getValue()[2]);
        assertTrue(stateJson.contains("\"pendingToolCalls\""), "state_json 必须持久化待执行工具批次");
        assertTrue(stateJson.contains("call_risk"));

        SseEvent waiting = events.stream()
                .filter(e -> EventType.WAITING_APPROVAL.equals(e.type()))
                .findFirst()
                .orElseThrow();
        assertTrue(waiting.data().contains("\"callId\":\"call_risk\""));
        assertTrue(waiting.data().contains("\"tool\":\"asset_delete\""));
        assertTrue(waiting.data().contains("\"args\":{\"id\":1}"));
        assertTrue(waiting.data().contains("\"approvalId\":88"));
    }

    @Test
    void 审批通过立即续跑且重复审批只生效一次() {
        when(jdbc.queryForObject(contains("tenant_id=?::bigint"), eq(Integer.class), any(Object[].class)))
                .thenReturn(1);
        when(jdbc.queryForObject(contains("SELECT step_index"), eq(Integer.class), any(Object[].class)))
                .thenReturn(2);
        when(jdbc.queryForObject(contains("FROM t_approval a"), eq(Integer.class), any(Object[].class)))
                .thenReturn(0);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1, 1, 0);

        assertTrue(service.decideLatest("1", "run-1", "alice", true).block());
        verify(runService, times(1)).resumeAfterApproval("run-1");

        // 第二次审批：t_approval CAS 返回 0，不允许再次续跑
        AgentException e = assertThrows(AgentException.class,
                () -> service.decideLatest("1", "run-1", "alice", true).block());
        assertEquals("APPROVAL_NOT_FOUND", e.getCode());
        verify(runService, times(1)).resumeAfterApproval("run-1");
    }

    @Test
    void 审批驳回转为取消并释放租约() {
        when(jdbc.queryForObject(contains("tenant_id=?::bigint"), eq(Integer.class), any(Object[].class)))
                .thenReturn(1);
        when(jdbc.queryForObject(contains("SELECT step_index"), eq(Integer.class), any(Object[].class)))
                .thenReturn(2);
        when(jdbc.queryForObject(contains("FROM t_approval a"), eq(Integer.class), any(Object[].class)))
                .thenReturn(0);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);

        assertTrue(service.decideLatest("1", "run-1", "alice", false).block());

        verify(jdbc).update(contains("state='CANCELED'"), any(Object[].class));
        verify(jdbc).update(contains("lease_until=now()"), any(Object[].class));
        verify(runService).rejectAfterApproval("run-1");
        verify(runService, never()).resumeAfterApproval(anyString());
    }

    @Test
    void 跨租户审批被拒() {
        when(jdbc.queryForObject(contains("tenant_id=?::bigint"), eq(Integer.class), any(Object[].class)))
                .thenReturn(0);

        AgentException e = assertThrows(AgentException.class,
                () -> service.decideLatest("2", "run-1", "mallory", true).block());
        assertEquals("RUN_NOT_FOUND", e.getCode());
        verify(jdbc, never()).update(anyString(), any(Object[].class));
        verify(runService, never()).resumeAfterApproval(anyString());
    }

    @Test
    void 超时未审批收敛为_TIMEOUT_并释放资源() {
        when(jdbc.queryForList(contains("JOIN t_agent_run"), eq(String.class), any(Object[].class)))
                .thenReturn(List.of("run-1"));
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);

        assertEquals(1, service.expireStale(50).block());
        verify(jdbc).update(contains("status='TIMEOUT'"), any(Object[].class));
        verify(jdbc).update(contains("state='TIMEOUT'"), any(Object[].class));
        verify(runService).expireAfterApproval("run-1");
    }
}
