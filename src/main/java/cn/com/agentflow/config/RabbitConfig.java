package cn.com.agentflow.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MQ 拓扑：
 * - run 审计队列：审计写入经 MQ 削峰后落库（见 audit/RunAuditService）；
 * - 报表任务队列：工具先落库再投递，由 consumer/ReportTaskConsumer 异步消费；
 * 两个主队列都挂死信，避免毒消息或重试耗尽的消息阻塞主队列。
 */
@Configuration
public class RabbitConfig {

    public static final String RUN_EXCHANGE = "af.run.exchange";
    public static final String AUDIT_QUEUE = "af.run.audit";
    public static final String AUDIT_ROUTING_KEY = "run.audit";

    /** 报表交换机与路由键沿用 tools/ReportTools 既有投递名，避免生产端与消费端两套拓扑 */
    public static final String REPORT_EXCHANGE = "report.exchange";
    public static final String REPORT_QUEUE = "report.queue";
    public static final String REPORT_ROUTING_KEY = "report.generate";
    /** 报表死信：重试耗尽或无法解析的消息在这里留存，人工排障后可按需重投 */
    public static final String REPORT_DLX = "report.exchange.dlx";
    public static final String REPORT_DLQ = "report.queue.dlq";
    public static final String REPORT_DLQ_ROUTING_KEY = "report.generate.dlq";

    @Bean
    public TopicExchange runExchange() {
        return new TopicExchange(RUN_EXCHANGE, true, false);
    }

    @Bean
    public Queue auditQueue() {
        return QueueBuilder.durable(AUDIT_QUEUE)
                .withArgument("x-dead-letter-exchange", RUN_EXCHANGE + ".dlx")
                .build();
    }

    @Bean
    public Binding auditBinding(Queue auditQueue, TopicExchange runExchange) {
        return BindingBuilder.bind(auditQueue).to(runExchange).with(AUDIT_ROUTING_KEY);
    }

    @Bean
    public TopicExchange reportExchange() {
        return new TopicExchange(REPORT_EXCHANGE, true, false);
    }

    @Bean
    public TopicExchange reportDeadLetterExchange() {
        return new TopicExchange(REPORT_DLX, true, false);
    }

    /**
     * 报表主队列只在此处声明：参数必须与运行中的队列完全一致，
     * 否则 RabbitMQ 会报 PRECONDITION_FAILED；DLX 参数随声明一起下发。
     */
    @Bean
    public Queue reportQueue() {
        return QueueBuilder.durable(REPORT_QUEUE)
                .withArgument("x-dead-letter-exchange", REPORT_DLX)
                .withArgument("x-dead-letter-routing-key", REPORT_DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Queue reportDeadLetterQueue() {
        return QueueBuilder.durable(REPORT_DLQ).build();
    }

    @Bean
    public Binding reportBinding(Queue reportQueue, TopicExchange reportExchange) {
        return BindingBuilder.bind(reportQueue).to(reportExchange).with(REPORT_ROUTING_KEY);
    }

    @Bean
    public Binding reportDeadLetterBinding(Queue reportDeadLetterQueue, TopicExchange reportDeadLetterExchange) {
        return BindingBuilder.bind(reportDeadLetterQueue).to(reportDeadLetterExchange).with(REPORT_DLQ_ROUTING_KEY);
    }
}
