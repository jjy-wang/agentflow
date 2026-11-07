package cn.com.agentflow;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 应用入口。
 * 扫描配置属性与 MyBatis Mapper，开启定时任务（租约续约/恢复任务）。
 */
@EnableScheduling
@SpringBootApplication
@ConfigurationPropertiesScan
@MapperScan("cn.com.agentflow.mapper")
public class AgentFlowApplication {

    public static void main(String[] args) {
        SpringApplication.run(AgentFlowApplication.class, args);
    }
}
