package cn.com.agentflow.consumer;

import cn.com.agentflow.config.RabbitConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 报表任务消费者。
 *
 * <p>运行在 Spring AMQP 的消费线程上（阻塞模型），因此这里直接同步访问 JdbcTemplate，
 * 不包 Mono/boundedElastic：那样只是把阻塞丢给另一个线程池，既不提升吞吐也掩盖了真实线程模型。</p>
 *
 * <p>幂等：以 report_task(task_id) 为唯一键，PENDING --CAS--> RUNNING 只有一个消费者能成功；
 * 重复投递时若已是 SUCCEEDED/FAILED/RUNNING 直接 ACK 跳过，不会重复生成报表。</p>
 *
 * <p>失败处理：可重试错误在原线程同步重试 maxRetry 次，耗尽后标 FAILED 并 nack 进死信；
 * 不可重试错误直接标 FAILED 并 ACK，避免毒消息在主队列无限循环。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReportTaskConsumer {

    /** 消费结果：决定 ACK 还是 nack 进死信 */
    public enum ConsumeOutcome {
        ACK_SUCCEEDED,
        ACK_SKIPPED,
        ACK_FAILED,
        DEAD_LETTER
    }

    private final ReportTaskStore store;
    private final ReportGenerator generator;
    private final ReportRetryPolicy retryPolicy;
    private final ObjectMapper mapper;

    @RabbitListener(queues = RabbitConfig.REPORT_QUEUE)
    public void onMessage(@Payload String body, Message message, Channel channel) {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        ConsumeOutcome outcome;
        try {
            outcome = dispatch(body);
        } catch (Exception e) {
            // 未预期异常（如 DB 不可用）不留在主队列：转入死信，由运维确认后重投
            log.error("报表任务消费异常，转入死信: tag={}", deliveryTag, e);
            outcome = ConsumeOutcome.DEAD_LETTER;
        }
        try {
            if (outcome == ConsumeOutcome.DEAD_LETTER) {
                channel.basicNack(deliveryTag, false, false);
            } else {
                channel.basicAck(deliveryTag, false);
            }
        } catch (IOException e) {
            // ACK 失败时 channel 关闭后消息会重投，幂等状态机保证不会重复生成
            log.error("报表任务 ACK 失败，消息将重投: tag={} outcome={}", deliveryTag, outcome, e);
        }
    }

    /** 抽成独立方法便于单元测试；不依赖 Channel，返回值即 ACK/死信决策 */
    public ConsumeOutcome dispatch(String raw) {
        ReportTaskMessage message;
        try {
            message = ReportTaskMessage.parse(mapper, raw);
        } catch (ReportTaskMessage.InvalidException e) {
            log.error("报表任务消息无法解析，转入死信: reason={} raw={}", e.getMessage(), abbreviate(raw));
            return ConsumeOutcome.DEAD_LETTER;
        }

        String taskId = message.taskId();
        if (taskId == null || taskId.isBlank()) {
            log.error("报表任务缺少 taskId，转入死信: raw={}", abbreviate(raw));
            return ConsumeOutcome.DEAD_LETTER;
        }

        try {
            message.validate();
        } catch (ReportTaskMessage.InvalidException e) {
            log.error("报表任务参数非法，标记 FAILED: taskId={} reason={}", taskId, e.getMessage());
            return markFailed(taskId) ? ConsumeOutcome.ACK_FAILED : ConsumeOutcome.DEAD_LETTER;
        }

        if (!store.claim(taskId)) {
            String status = store.findStatus(taskId);
            if (status == null) {
                // 记录不存在属于不可重试错误，ACK 丢弃，避免同一毒消息反复投递
                log.error("报表任务记录不存在，ACK 丢弃: taskId={}", taskId);
                return ConsumeOutcome.ACK_FAILED;
            }
            // SUCCEEDED/FAILED 是终态；RUNNING 说明其他消费线程已认领，跳过保证幂等
            log.info("报表任务无需重复处理，跳过: taskId={} status={}", taskId, status);
            return ConsumeOutcome.ACK_SKIPPED;
        }

        return execute(message);
    }

    private ConsumeOutcome execute(ReportTaskMessage message) {
        String taskId = message.taskId();
        int retriesDone = 0;
        while (true) {
            try {
                generator.generate(message);
                store.markSucceeded(taskId);
                log.info("报表任务完成: taskId={} attempts={}", taskId, retriesDone + 1);
                return ConsumeOutcome.ACK_SUCCEEDED;
            } catch (Exception e) {
                ReportRetryPolicy.Decision decision = retryPolicy.decide(retriesDone, e);
                if (decision == ReportRetryPolicy.Decision.RETRY) {
                    retriesDone++;
                    log.warn("报表任务执行失败，同步重试 {}/{}: taskId={} reason={}",
                            retriesDone, retryPolicy.maxRetry(), taskId, e.toString());
                    continue;
                }
                if (decision == ReportRetryPolicy.Decision.DEAD_LETTER) {
                    log.error("报表任务重试耗尽，标记 FAILED 并转入死信: taskId={} attempts={}",
                            taskId, retriesDone + 1, e);
                    markFailed(taskId);
                    return ConsumeOutcome.DEAD_LETTER;
                }
                log.error("报表任务遇到不可重试错误，标记 FAILED: taskId={} attempts={}",
                        taskId, retriesDone + 1, e);
                return markFailed(taskId) ? ConsumeOutcome.ACK_FAILED : ConsumeOutcome.DEAD_LETTER;
            }
        }
    }

    /** 状态更新失败时不吞异常语义：返回 false 让调用方保留消息（死信）而不是 ACK 后任务卡在 RUNNING */
    private boolean markFailed(String taskId) {
        try {
            store.markFailed(taskId);
            return true;
        } catch (RuntimeException e) {
            log.error("报表任务状态更新为 FAILED 失败: taskId={}", taskId, e);
            return false;
        }
    }

    private static String abbreviate(String raw) {
        if (raw == null) {
            return "null";
        }
        return raw.length() <= 256 ? raw : raw.substring(0, 256) + "...";
    }
}
