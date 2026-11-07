package cn.com.agentflow.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.web.reactive.config.CorsRegistry;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/** WebFlux 全局配置：CORS 放开 API 域名，编解码内存上限对齐 SSE 场景 */
@Configuration
public class WebFluxConfig implements WebFluxConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        // 企业内网前端联调用；生产应在网关层收敛，这里保持最小可用
        registry.addMapping("/v1/**")
                .allowedOriginPatterns("*")
                .allowedMethods("GET", "POST", "OPTIONS")
                .allowedHeaders("*")
                .maxAge(3600);
    }

    @Override
    public void configureHttpMessageCodecs(ServerCodecConfigurer configurer) {
        // 默认 256KB 对大 system prompt + 历史的消息体偏小，提到 1MB
        configurer.defaultCodecs().maxInMemorySize(1024 * 1024);
    }
}
