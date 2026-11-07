package cn.com.agentflow.llm;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.metric.AgentMetrics;
import cn.com.agentflow.quota.ModelBulkhead;
import cn.com.agentflow.quota.QuotaReservationLedger;
import cn.com.agentflow.quota.QuotaService;
import cn.com.agentflow.quota.TokenReservation;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * fallback 链 + 本地预扣归还 + 错误码语义：
 * 准入失败不扣、下游失败归还一次、主模型 5xx 切 fallback、全失败才报错、舱壁满报 MODEL_BUSY。
 */
class LlmInvokerFallbackTest {

    private static final int MAX_TOKENS = 32;

    private final AgentMetrics metrics = new AgentMetrics(new SimpleMeterRegistry());

    @Test
    void 本地预检拒绝返回RATE_LIMITED而不是NO_MODEL_AVAILABLE() {
        QuotaService quota = mock(QuotaService.class);
        grantSettle(quota);
        // 本地令牌桶拒绝：reason 带 LOCAL_ 前缀
        when(quota.reserve(anyString(), anyInt(), anyInt(), anyInt()))
                .thenReturn(Mono.just(TokenReservation.deny("mock-large", "LOCAL_TPM_EXCEEDED")));

        ModelPort primary = port("mock-large", Flux.just(new ChatChunk.Finished("stop")));
        LlmInvoker invoker = invoker(router(plan(target("mock-large", primary))), quota);

        StepVerifier.create(invoker.invoke(request()))
                .expectErrorSatisfies(e -> {
                    AgentException ae = (AgentException) e;
                    assertEquals("RATE_LIMITED", ae.getCode(),
                            "本地限流拒绝必须报 RATE_LIMITED，不能伪装成 NO_MODEL_AVAILABLE");
                })
                .verify();

        // 准入失败没有扣减，绝不能退款
        verify(quota, never()).refundLocal(anyString(), anyInt());
    }

    @Test
    void Redis配额拒绝返回QUOTA_EXCEEDED() {
        QuotaService quota = mock(QuotaService.class);
        grantSettle(quota);
        when(quota.reserve(anyString(), anyInt(), anyInt(), anyInt()))
                .thenReturn(Mono.just(TokenReservation.deny("mock-large", "TPM_EXCEEDED")));

        ModelPort primary = port("mock-large", Flux.just(new ChatChunk.Finished("stop")));
        LlmInvoker invoker = invoker(router(plan(target("mock-large", primary))), quota);

        StepVerifier.create(invoker.invoke(request()))
                .expectErrorSatisfies(e ->
                        assertEquals("QUOTA_EXCEEDED", ((AgentException) e).getCode()))
                .verify();
    }

    @Test
    void 主模型5xx时切到fallback并最终成功且只退一次本地令牌() {
        QuotaService quota = mock(QuotaService.class);
        grantSettle(quota);
        when(quota.reserve(anyString(), anyInt(), anyInt(), anyInt()))
                .thenAnswer(inv -> Mono.just(new TokenReservation(
                        inv.getArgument(0), inv.getArgument(3), true, "OK")));

        ModelPort primary = port("mock-large", serverError());
        ModelPort backup = port("mock-small",
                Flux.just(new ChatChunk.Usage(10, 5), new ChatChunk.Finished("stop")));
        LlmInvoker invoker = invoker(router(plan(
                target("mock-large", primary), target("mock-small", backup))), quota);

        StepVerifier.create(invoker.invoke(request()))
                .expectNextCount(2)
                .verifyComplete();

        // 失败的中间尝试按 0 结算并归还本地预扣，成功调用按实际 15 结算：
        // 同一请求只计费一次，成功调用的 maxTokens 预扣余量也会退回本地桶
        verify(quota, times(1)).refundLocal(eq("mock-large"), anyInt());
        verify(quota, times(1)).refundLocal(eq("mock-small"), anyInt());
        verify(quota).settle(eq("mock-large"), anyInt(), anyInt(), eq(0));
        verify(quota).settle(eq("mock-small"), anyInt(), anyInt(), eq(15));
    }

    @Test
    void fallback链全部失败才返回UPSTREAM_ERROR() {
        QuotaService quota = mock(QuotaService.class);
        grantSettle(quota);
        when(quota.reserve(anyString(), anyInt(), anyInt(), anyInt()))
                .thenAnswer(inv -> Mono.just(new TokenReservation(
                        inv.getArgument(0), inv.getArgument(3), true, "OK")));

        ModelPort primary = port("mock-large", serverError());
        ModelPort backup = port("mock-small", serverError());
        LlmInvoker invoker = invoker(router(plan(
                target("mock-large", primary), target("mock-small", backup))), quota);

        StepVerifier.create(invoker.invoke(request()))
                .expectErrorSatisfies(e -> {
                    AgentException ae = (AgentException) e;
                    assertEquals("UPSTREAM_ERROR", ae.getCode(),
                            "所有候选都真正调用过，错误码必须是上游失败而不是没有模型");
                })
                .verify();

        // 两条失败路径各自只归还一次
        verify(quota, times(1)).refundLocal(eq("mock-large"), anyInt());
        verify(quota, times(1)).refundLocal(eq("mock-small"), anyInt());
    }

    @Test
    void 舱壁已满返回MODEL_BUSY() {
        QuotaService quota = mock(QuotaService.class);
        grantSettle(quota);
        when(quota.reserve(anyString(), anyInt(), anyInt(), anyInt()))
                .thenAnswer(inv -> Mono.just(new TokenReservation(
                        inv.getArgument(0), inv.getArgument(3), true, "OK")));

        AgentFlowProperties.Model config = config(0);
        ModelPort primary = port("mock-large", Flux.just(new ChatChunk.Finished("stop")));
        LlmInvoker invoker = invoker(router(plan(
                new ModelRouter.RouteTarget("mock-large", primary, config))), quota);

        StepVerifier.create(invoker.invoke(request()))
                .expectErrorSatisfies(e ->
                        assertEquals("MODEL_BUSY", ((AgentException) e).getCode()))
                .verify();
    }

    private LlmInvoker invoker(ModelRouter router, QuotaService quota) {
        QuotaReservationLedger ledger = mock(QuotaReservationLedger.class);
        when(ledger.begin(anyString(), anyString(), anyInt())).thenReturn(Mono.just(1L));
        when(ledger.markReserved(anyLong(), anyInt())).thenReturn(Mono.empty());
        when(ledger.markSettled(anyLong(), anyInt())).thenReturn(Mono.empty());
        return new LlmInvoker(router, quota, new ModelBulkhead(), metrics, ledger);
    }

    private ModelRouter router(ModelRouter.RoutePlan plan) {
        ModelRouter router = mock(ModelRouter.class);
        when(router.route(any(), any(), anyInt())).thenReturn(plan);
        when(router.allowRequest(anyString())).thenReturn(true);
        return router;
    }

    private void grantSettle(QuotaService quota) {
        when(quota.settle(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(Mono.empty());
        when(quota.refundLocal(anyString(), anyInt())).thenReturn(true);
    }

    private ModelRouter.RoutePlan plan(ModelRouter.RouteTarget... targets) {
        return new ModelRouter.RoutePlan(List.of(targets), List.of(), targets.length);
    }

    private ModelRouter.RouteTarget target(String name, ModelPort port) {
        return new ModelRouter.RouteTarget(name, port, config(4));
    }

    private AgentFlowProperties.Model config(int maxConcurrency) {
        AgentFlowProperties.Model config = new AgentFlowProperties.Model();
        config.setRpmLimit(600);
        config.setTpmLimit(200_000);
        config.setMaxConcurrency(maxConcurrency);
        config.setTimeoutMs(5_000);
        config.setTtftTimeoutMs(5_000);
        return config;
    }

    private ChatRequest request() {
        return new ChatRequest("mock-large", List.of(ChatMessage.user("你好")), List.of(), MAX_TOKENS, 0.2);
    }

    private Flux<ChatChunk> serverError() {
        return Flux.error(WebClientResponseException.create(500, "Internal Server Error",
                HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8));
    }

    private ModelPort port(String name, Flux<ChatChunk> stream) {
        return new ModelPort() {
            @Override
            public String modelName() {
                return name;
            }

            @Override
            public int contextWindow() {
                return 8192;
            }

            @Override
            public Flux<ChatChunk> stream(ChatRequest request) {
                return stream;
            }
        };
    }
}
