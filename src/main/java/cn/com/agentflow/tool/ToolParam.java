package cn.com.agentflow.tool;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 工具参数注解：描述单个入参，用于反射生成 JSON Schema
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ToolParam {
    String name();

    String description();

    boolean required() default false;
}
