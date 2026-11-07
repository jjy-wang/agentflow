package cn.com.agentflow.tool;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 工具注解：标注在方法上即注册为一个 Agent 可调用工具
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Tool {
    String name();

    String description();

    /** 1 低风险（只读）2 中风险（写）3 高风险（必须人工审批） */
    int riskLevel() default 1;

    int timeoutMs() default 5000;
}
