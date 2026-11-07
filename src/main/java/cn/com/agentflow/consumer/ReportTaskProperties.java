package cn.com.agentflow.consumer;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 报表异步任务配置（agentflow.report）。
 */
@Data
@Component
@ConfigurationProperties(prefix = "agentflow.report")
public class ReportTaskProperties {

    /** 首次执行失败后的最大重试次数，总尝试次数 = maxRetry + 1 */
    private int maxRetry = 3;

    /** 报表数据产物目录，按 taskId 覆盖写入，重复消费不会产生多份文件 */
    private String outputDir = "./data/reports";
}
