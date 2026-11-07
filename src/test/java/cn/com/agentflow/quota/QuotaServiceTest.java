package cn.com.agentflow.quota;

import cn.com.agentflow.config.AgentFlowProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** QuotaService：RPM/TPM 原子预扣（Lua）、降级放行、结算退还（md 18.1） */
@ExtendWith(MockitoExtension.class)
class QuotaServiceTest {

    @Mock
    private StringRedisTemplate redis;

    private QuotaService quotaService;

    @BeforeEach
    void setUp() {
        quotaService = new QuotaService(redis,
                new LocalRateLimiter(new SimpleMeterRegistry()), new AgentFlowProperties());
    }

    @Test
    void 预扣成功_granted为true() {
        doReturn(List.of(1L, "OK", 12L)).when(redis)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString());
        StepVerifier.create(quotaService.reserve("mock-large", 600, 200_000, 1000))
                .assertNext(r -> {
                    assertTrue(r.granted());
                    assertEquals("OK", r.reason());
                })
                .verifyComplete();
    }

    @Test
    void TPM不足_返回拒绝原因() {
        doReturn(List.of(0L, "TPM_EXCEEDED", 0L)).when(redis)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString());
        StepVerifier.create(quotaService.reserve("mock-large", 600, 100, 80))
                .assertNext(r -> {
                    assertFalse(r.granted());
                    assertEquals("TPM_EXCEEDED", r.reason());
                })
                .verifyComplete();
    }

    @Test
    void redis异常时降级放行_默认允许() {
        doThrow(new RuntimeException("connection refused")).when(redis)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString());
        StepVerifier.create(quotaService.reserve("mock-large", 600, 200_000, 1000))
                .assertNext(r -> {
                    assertTrue(r.granted(), "Redis 故障必须降级放行（fail-open）");
                    assertEquals("DEGRADED", r.reason());
                })
                .verifyComplete();
    }

    @Test
    void 本地预检超阈值直接拒绝_不打Redis() {
        // 本地预检容量 = tpmLimit * 1.2 = 120，申请 1000 必拒
        StepVerifier.create(quotaService.reserve("mock-large", 600, 100, 1000))
                .assertNext(r -> {
                    assertFalse(r.granted());
                    assertEquals("LOCAL_TPM_EXCEEDED", r.reason());
                })
                .verifyComplete();
        verifyNoInteractions(redis);
    }

    @Test
    void 结算时把多预扣的token退还() {
        doReturn(1L).when(redis)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString());
        StepVerifier.create(quotaService.settle("mock-large", 1000, 100, 40))
                .verifyComplete();
        verify(redis).execute(any(RedisScript.class),
                eq(List.of("quota:tpm:mock-large")), eq("60"), eq("1000"));
    }

    @Test
    void 实耗不低于预留时无需退还() {
        StepVerifier.create(quotaService.settle("mock-large", 1000, 100, 100))
                .verifyComplete();
        verifyNoInteractions(redis);
    }

    @Test
    void 对账退款复用settle脚本退回预扣量() {
        doReturn(1L).when(redis)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString());

        quotaService.refundReserved("mock-large", 1000, 60);

        verify(redis).execute(any(RedisScript.class),
                eq(List.of("quota:tpm:mock-large")), eq("60"), eq("1000"));
    }

    @Test
    void 对账退款量为零时不打Redis() {
        quotaService.refundReserved("mock-large", 1000, 0);
        verifyNoInteractions(redis);
    }
}
