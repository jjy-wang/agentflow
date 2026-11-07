package cn.com.agentflow.memory;

import cn.com.agentflow.config.AgentFlowProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 长期记忆预检索：把本轮用户问题转向量后做 pgvector 召回，
 * 渲染成 system prompt 的独立小节。召回是增强能力，任何异常都降级为“无记忆”，
 * 绝不允许向量库故障导致 run 失败。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MemoryRetrievalService {

    /** 单条记忆注入上限，避免召回内容反向撑爆上下文预算 */
    private static final int MAX_ITEM_CHARS = 500;

    private final EmbeddingService embeddingService;
    private final VectorMemory vectorMemory;
    private final AgentFlowProperties props;

    /**
     * 召回并渲染记忆小节；关闭开关、无 query、无命中均返回空串。
     *
     * @param tenantId 租户 ID（来自 RunContext，不接受模型传参）
     * @param query    本轮用户问题
     */
    public Mono<String> recallSection(String tenantId, String query) {
        AgentFlowProperties.MemoryPreRetrieval config = props.getAgent().getMemoryPreRetrieval();
        if (!config.isEnabled() || query == null || query.isBlank()) {
            return Mono.just("");
        }
        long tenant;
        try {
            tenant = Long.parseLong(tenantId);
        } catch (NumberFormatException e) {
            log.warn("预检索跳过：租户 ID 非法 tenantId={}", tenantId);
            return Mono.just("");
        }
        return embeddingService.embed(query)
                .flatMap(vector -> vectorMemory.recall(tenant, vector, config.getTopK()))
                .map(this::render)
                .defaultIfEmpty("")
                .onErrorResume(e -> {
                    log.warn("长期记忆预检索失败，降级为无记忆: tenantId={} err={}", tenantId, e.toString());
                    return Mono.just("");
                });
    }

    private String render(List<VectorMemory.MemoryItem> items) {
        if (items == null || items.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(256);
        sb.append("\n\n【长期记忆·自动召回】\n")
                .append("以下为与本轮问题相关的历史记忆，仅作参考；若与用户最新指令冲突，以用户最新指令为准。\n");
        int index = 1;
        for (VectorMemory.MemoryItem item : items) {
            String content = item.content() == null ? "" : item.content();
            if (content.length() > MAX_ITEM_CHARS) {
                content = content.substring(0, MAX_ITEM_CHARS) + "...";
            }
            sb.append(index++).append(". (").append(String.format("%.2f", item.score()))
                    .append(") ").append(content).append('\n');
        }
        return sb.toString();
    }
}
