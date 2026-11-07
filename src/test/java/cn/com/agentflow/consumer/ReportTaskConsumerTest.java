package cn.com.agentflow.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * 报表任务消费者：幂等、有限重试、死信与不可重试分支。
 * 不依赖真实 MQ 与 PG，消费决策通过 dispatch 返回值验证。
 */
@ExtendWith(MockitoExtension.class)
class ReportTaskConsumerTest {

    private static final String TASK_ID = "11111111-1111-1111-1111-111111111111";

    private final ObjectMapper mapper = new ObjectMapper();

    @Mock
    private ReportTaskStore store;
    @Mock
    private ReportGenerator generator;

    private ReportTaskProperties properties;
    private ReportTaskConsumer consumer;

    @BeforeEach
    void setUp() {
        properties = new ReportTaskProperties();
        properties.setMaxRetry(3);
        consumer = new ReportTaskConsumer(store, generator, new ReportRetryPolicy(properties), mapper);
    }

    private String payload(String taskId) {
        return """
                {"taskId":"%s","format":"pdf","assetIds":["asset-1","asset-2"]}
                """.formatted(taskId);
    }

    @Test
    void 同一taskId重复投递只处理一次() throws Exception {
        when(store.claim(TASK_ID)).thenReturn(true, false);
        when(store.findStatus(TASK_ID)).thenReturn(ReportTaskStore.SUCCEEDED);

        assertEquals(ReportTaskConsumer.ConsumeOutcome.ACK_SUCCEEDED, consumer.dispatch(payload(TASK_ID)));
        assertEquals(ReportTaskConsumer.ConsumeOutcome.ACK_SKIPPED, consumer.dispatch(payload(TASK_ID)));

        verify(generator, times(1)).generate(any());
        verify(store, times(1)).markSucceeded(TASK_ID);
    }

    @Test
    void 可重试失败达上限后标记FAILED并进入死信() throws Exception {
        when(store.claim(TASK_ID)).thenReturn(true);
        doThrow(new ReportRetryPolicy.RetryableException("下游 503"))
                .when(generator).generate(any());

        assertEquals(ReportTaskConsumer.ConsumeOutcome.DEAD_LETTER, consumer.dispatch(payload(TASK_ID)));

        // 首次 + maxRetry(3) 次重试 = 4 次尝试
        verify(generator, times(4)).generate(any());
        verify(store, times(1)).markFailed(TASK_ID);
        verify(store, never()).markSucceeded(anyString());
    }

    @Test
    void 不可重试错误直接标记FAILED并ACK() throws Exception {
        when(store.claim(TASK_ID)).thenReturn(true);
        doThrow(new IllegalArgumentException("format 不支持"))
                .when(generator).generate(any());

        assertEquals(ReportTaskConsumer.ConsumeOutcome.ACK_FAILED, consumer.dispatch(payload(TASK_ID)));

        verify(generator, times(1)).generate(any());
        verify(store, times(1)).markFailed(TASK_ID);
    }

    @Test
    void 状态机CAS并发下不重复处理() throws Exception {
        AtomicBoolean claimed = new AtomicBoolean(false);
        when(store.claim(TASK_ID)).thenAnswer(invocation -> claimed.compareAndSet(false, true));
        when(store.findStatus(TASK_ID)).thenReturn(ReportTaskStore.RUNNING);

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
            return null;
        }).when(generator).generate(any());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<ReportTaskConsumer.ConsumeOutcome> first = pool.submit(() -> consumer.dispatch(payload(TASK_ID)));
            entered.await(5, TimeUnit.SECONDS);
            Future<ReportTaskConsumer.ConsumeOutcome> second = pool.submit(() -> consumer.dispatch(payload(TASK_ID)));

            assertEquals(ReportTaskConsumer.ConsumeOutcome.ACK_SKIPPED, second.get(5, TimeUnit.SECONDS));
            release.countDown();
            assertEquals(ReportTaskConsumer.ConsumeOutcome.ACK_SUCCEEDED, first.get(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        // 两个线程只有一个 CAS 成功，报表只生成一次
        verify(generator, times(1)).generate(any());
        verify(store, times(1)).markSucceeded(TASK_ID);
    }

    @Test
    void 记录不存在属于不可重试错误直接ACK丢弃() {
        when(store.claim(TASK_ID)).thenReturn(false);
        when(store.findStatus(TASK_ID)).thenReturn(null);

        assertEquals(ReportTaskConsumer.ConsumeOutcome.ACK_FAILED, consumer.dispatch(payload(TASK_ID)));
        verifyNoInteractions(generator);
    }

    @Test
    void 参数非法直接标记FAILED并ACK() {
        String raw = """
                {"taskId":"%s","format":"exe","assetIds":["asset-1"]}
                """.formatted(TASK_ID);

        assertEquals(ReportTaskConsumer.ConsumeOutcome.ACK_FAILED, consumer.dispatch(raw));

        verify(store, times(1)).markFailed(TASK_ID);
        verify(store, never()).claim(anyString());
        verifyNoInteractions(generator);
    }

    @Test
    void 消息无法解析时进入死信() {
        assertEquals(ReportTaskConsumer.ConsumeOutcome.DEAD_LETTER, consumer.dispatch("not-json"));

        verifyNoInteractions(store);
        verifyNoInteractions(generator);
    }

    @Test
    void 重试策略边界_额度耗尽转死信_非可重试快速失败() {
        ReportRetryPolicy policy = new ReportRetryPolicy(properties);

        assertEquals(ReportRetryPolicy.Decision.RETRY,
                policy.decide(2, new ReportRetryPolicy.RetryableException("临时故障")));
        assertEquals(ReportRetryPolicy.Decision.DEAD_LETTER,
                policy.decide(3, new ReportRetryPolicy.RetryableException("临时故障")));
        assertEquals(ReportRetryPolicy.Decision.FAIL_FAST,
                policy.decide(0, new IllegalArgumentException("参数非法")));
    }
}
