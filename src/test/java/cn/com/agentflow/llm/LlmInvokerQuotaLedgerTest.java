package cn.com.agentflow.llm;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.mapper.QuotaReservationMapper;
import cn.com.agentflow.metric.AgentMetrics;
import cn.com.agentflow.quota.ModelBulkhead;
import cn.com.agentflow.quota.QuotaReservationLedger;
import cn.com.agentflow.quota.QuotaService;
import cn.com.agentflow.quota.TokenReservation;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 账本写失败只影响补偿，不影响 LLM 主链路 */
class LlmInvokerQuotaLedgerTest {

    @Test
    void 账本INIT写失败仍然正常调用模型() {
        AgentFlowProperties.Model config = new AgentFlowProperties.Model();
        config.setRpmLimit(600);
        config.setTpmLimit(200000);
        config.setMaxConcurrency(4);

        ModelPort port = new ModelPort() {
            @Override
            public String modelName() {
                return "mock-large";
            }

            @Override
            public int contextWindow() {
                return 8192;
            }

            @Override
            public Flux<ChatChunk> stream(ChatRequest request) {
                return Flux.just(new ChatChunk.Usage(10, 5), new ChatChunk.Finished("stop"));
            }
        };

        ModelRouter router = mock(ModelRouter.class);
        when(router.route(any(), any(), anyInt()))
                .thenReturn(new ModelRouter.RoutePlan(
                        List.of(new ModelRouter.RouteTarget("mock-large", port, config)),
                        List.of(), 3));
        when(router.allowRequest(anyString())).thenReturn(true);

        QuotaService quota = mock(QuotaService.class);
        when(quota.reserve(anyString(), anyInt(), anyInt(), anyInt()))
                .thenReturn(Mono.just(new TokenReservation("mock-large", 100, true, "OK")));
        when(quota.settle(anyString(), anyInt(), anyInt(), anyInt()))
                .thenReturn(Mono.<Void>empty());

        QuotaReservationMapper mapper = mock(QuotaReservationMapper.class);
        when(mapper.insertInit(any())).thenThrow(new RuntimeException("ledger db down"));
        QuotaReservationLedger ledger = new QuotaReservationLedger(mapper);

        LlmInvoker invoker = new LlmInvoker(router, quota, new ModelBulkhead(),
                new AgentMetrics(new SimpleMeterRegistry()), ledger);

        ChatRequest request = new ChatRequest("mock-large",
                List.of(ChatMessage.user("你好")), List.of(), 32, 0.2);

        StepVerifier.create(invoker.invoke(request))
                .expectNextCount(2)
                .verifyComplete();
    }
}
