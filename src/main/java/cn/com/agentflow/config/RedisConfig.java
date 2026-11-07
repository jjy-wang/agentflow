package cn.com.agentflow.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * Redis 与 JSON 基础设施。
 * 阻塞式 StringRedisTemplate（配额 Lua）与 ReactiveStringRedisTemplate（租约、缓存）
 * 共用 Boot 自动装配的连接工厂，这里只补响应式模板与统一 ObjectMapper。
 */
@Configuration
public class RedisConfig {

    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory factory) {
        return new ReactiveStringRedisTemplate(factory);
    }

    /** 全局 ObjectMapper：忽略未知字段，兼容 state_json 演进时新增字段 */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }
}
