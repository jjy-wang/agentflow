package cn.com.agentflow.run;

import cn.com.agentflow.agent.AgentRuntime;
import cn.com.agentflow.api.dto.RunRequest;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.mapper.RunMapper;
import cn.com.agentflow.metric.AgentMetrics;
import cn.com.agentflow.stream.RunEventBus;
import cn.com.agentflow.stream.SseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * run 超时复用统一收尾路径：租约释放（RunMapper.finish 置 lease_until=now()）、
 * SSE sink 关闭、RUN_FAILED 事件带 code/elapsedMs。
 */
class RunServiceTimeoutTest {

    @Test
    void run超时落库TIMEOUT并关闭SSE_事件带错误码与耗时() {
        RunMapper runMapper = mock(RunMapper.class);
        AgentRuntime runtime = mock(AgentRuntime.class);
        RunEventBus eventBus = mock(RunEventBus.class);
        AgentMetrics metrics = mock(AgentMetrics.class);

        Sinks.Many<SseEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
        List<SseEvent> events = new CopyOnWriteArrayList<>();
        sink.asFlux().subscribe(events::add);
        when(eventBus.create(anyString())).thenReturn(sink);
        when(runMapper.casState(anyString(), any(), any(), any(), anyInt())).thenReturn(1);
        when(runtime.execute(any())).thenReturn(Mono.error(new AgentException("RUN_TIMEOUT",
                "run 总时长超过预算 300000ms，已用 300010ms")));

        RunService service = new RunService(runMapper, runtime, eventBus, metrics,
                new AgentFlowProperties(), new ObjectMapper());

        RunRequest req = new RunRequest();
        req.setQuery("查询 10.0.0.1 主机");
        req.setSessionId(100L);
        RunService.AgentRunView view = service.create("tenant-1", req).block();

        // 收尾路径：落库即释放租约，错误码原样透传
        verify(runMapper).finish(eq(view.runId()), eq(RunState.TIMEOUT), eq("RUN_TIMEOUT"),
                contains("超过预算"), anyInt(), anyInt(), anyString());
        // 收尾路径：SSE sink 关闭
        verify(eventBus).complete(view.runId());

        List<SseEvent> failed = events.stream()
                .filter(e -> "run.failed".equals(e.type()))
                .collect(Collectors.toList());
        assertEquals(1, failed.size(), "超时只发一条 run.failed 终态事件");
        assertTrue(failed.get(0).data().contains("\"state\":\"TIMEOUT\""));
        assertTrue(failed.get(0).data().contains("\"code\":\"RUN_TIMEOUT\""));
        assertTrue(failed.get(0).data().contains("\"elapsedMs\":"));
    }
}
