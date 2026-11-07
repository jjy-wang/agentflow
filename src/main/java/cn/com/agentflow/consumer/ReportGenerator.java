package cn.com.agentflow.consumer;

/**
 * 报表生成端口。消费者只依赖该接口，便于在无 MQ、无文件系统的单元测试中验证幂等与重试。
 */
public interface ReportGenerator {

    /** 生成报表数据产物；可重试故障抛 IOException 或 ReportRetryPolicy.RetryableException */
    void generate(ReportTaskMessage message) throws Exception;
}
