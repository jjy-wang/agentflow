package cn.com.agentflow.memory;

import cn.com.agentflow.config.AgentFlowProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * 向量嵌入服务：调用 OpenAI 兼容的 /v1/embeddings 端点。
 * 嵌入是增强能力不是主链路：任何异常都降级为空数组并记 warn，由调用方决定跳过记忆读写。
 */
@Slf4j
@Component
public class EmbeddingService {

    private final AgentFlowProperties props;
    private final ObjectMapper mapper;
    private final WebClient webClient;

    public EmbeddingService(AgentFlowProperties props, ObjectMapper mapper) {
        this.props = props;
        this.mapper = mapper;
        this.webClient = WebClient.builder()
                .baseUrl(props.getMemory().getEmbeddingBaseUrl())
                .defaultHeader("Authorization", "Bearer " + props.getMemory().getEmbeddingApiKey())
                .build();
    }

    /** 文本转向量；失败返回空数组（length == 0 表示不可用），不抛异常 */
    public Mono<float[]> embed(String text) {
        ObjectNode body = mapper.createObjectNode();
        body.put("model", props.getMemory().getEmbeddingModel());
        body.put("input", text);
        return Mono.defer(() -> webClient.post()
                        .uri("/v1/embeddings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(body)
                        .retrieve()
                        .bodyToMono(JsonNode.class))
                .timeout(Duration.ofMillis(props.getMemory().getEmbeddingTimeoutMs()))
                .map(this::extract)
                .onErrorResume(e -> {
                    log.warn("embedding 调用失败，降级为空向量: err={}", e.toString());
                    return Mono.just(new float[0]);
                });
    }

    /** 取 data[0].embedding；维度与配置不符视为不可用，避免写坏 vector(1024) 列 */
    private float[] extract(JsonNode root) {
        JsonNode embedding = root.path("data").path(0).path("embedding");
        int dim = props.getMemory().getEmbeddingDim();
        if (!embedding.isArray() || embedding.size() != dim) {
            log.warn("embedding 维度不符: expect={}, actual={}", dim, embedding.size());
            return new float[0];
        }
        float[] vector = new float[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = (float) embedding.get(i).asDouble();
        }
        return vector;
    }
}
