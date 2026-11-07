package cn.com.agentflow.tools;

import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.cache.EmbeddingCache;
import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.memory.EmbeddingService;
import cn.com.agentflow.memory.VectorMemory;
import cn.com.agentflow.tool.Tool;
import cn.com.agentflow.tool.ToolParam;
import cn.com.agentflow.tool.ToolResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * 长期记忆工具：让模型自主召回/写入 pgvector 记忆，形成 RAG 闭环。
 * ToolExecutor 已在 boundedElastic 上执行 handler，这里的阻塞等待不会占用事件循环。
 * 租户来自 RunContext 而非模型参数，避免模型伪造租户读写他人记忆。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MemoryTools {

    private static final int DEFAULT_TOP_K = 5;
    private static final int MAX_TOP_K = 20;
    /** 单条写入内容上限，防止模型把整段对话塞进向量库 */
    private static final int MAX_CONTENT_CHARS = 4000;

    private final EmbeddingService embeddingService;
    private final VectorMemory vectorMemory;
    private final EmbeddingCache embeddingCache;
    private final AgentFlowProperties props;
    private final ObjectMapper mapper;

    @Tool(name = "memory_recall",
            description = "从长期记忆库按语义召回与问题相关的历史事实。适用于需要回忆用户偏好、历史结论或既往排查结果时。",
            timeoutMs = 12000)
    public ToolResult recall(@ToolParam(name = "query", description = "要召回的内容，用自然语言描述", required = true) String query,
                             @ToolParam(name = "topK", description = "返回条数，1-20，默认 5") Integer topK,
                             RunContext ctx) {
        if (query == null || query.isBlank()) {
            return ToolResult.fail("MEMORY_QUERY_EMPTY", "query 不能为空");
        }
        Long tenantId = tenantId(ctx);
        if (tenantId == null) {
            return ToolResult.fail("TENANT_INVALID", "缺少合法的租户上下文");
        }
        int k = topK == null ? DEFAULT_TOP_K : Math.min(MAX_TOP_K, Math.max(1, topK));
        float[] vector = embed(query);
        List<VectorMemory.MemoryItem> items = vectorMemory.recall(tenantId, vector, k)
                .block(waitTimeout());
        if (items == null) {
            items = List.of();
        }
        ObjectNode root = mapper.createObjectNode();
        root.put("count", items.size());
        ArrayNode array = root.putArray("items");
        for (VectorMemory.MemoryItem item : items) {
            ObjectNode node = array.addObject();
            node.put("id", item.id());
            node.put("content", item.content());
            node.put("similarity", item.similarity());
            node.put("score", item.score());
        }
        return ToolResult.ok(root);
    }

    @Tool(name = "memory_write",
            description = "把值得长期记住的事实写入长期记忆库（如用户偏好、环境约束、已确认的结论）。对同一内容重复写入会自动去重。",
            riskLevel = 2, timeoutMs = 12000)
    public ToolResult write(@ToolParam(name = "content", description = "要记住的要点，简洁的事实陈述", required = true) String content,
                            @ToolParam(name = "tags", description = "逗号分隔的标签，便于分类，如 环境,数据库") String tags,
                            RunContext ctx) {
        if (content == null || content.isBlank()) {
            return ToolResult.fail("MEMORY_CONTENT_EMPTY", "content 不能为空");
        }
        if (content.length() > MAX_CONTENT_CHARS) {
            return ToolResult.fail("MEMORY_CONTENT_TOO_LONG",
                    "content 超过上限 " + MAX_CONTENT_CHARS + " 字符");
        }
        Long tenantId = tenantId(ctx);
        if (tenantId == null) {
            return ToolResult.fail("TENANT_INVALID", "缺少合法的租户上下文");
        }
        String stored = tags == null || tags.isBlank() ? content : content + "\n[标签] " + tags;
        float[] vector = embed(stored);
        if (vector.length == 0) {
            return ToolResult.fail("EMBEDDING_UNAVAILABLE", "向量服务不可用，本次记忆未写入");
        }
        vectorMemory.save(tenantId, ctx.getSessionId(), stored, vector, 1.0f)
                .block(waitTimeout());
        ObjectNode root = mapper.createObjectNode();
        root.put("saved", true);
        root.put("contentLength", content.length());
        root.put("tags", tags == null ? "" : tags);
        return ToolResult.ok(root);
    }

    /** 先查两级缓存，miss 再调用 embedding 服务；空向量表示降级，由调用方决定跳过 */
    private float[] embed(String text) {
        float[] vector = embeddingCache.get(text)
                .switchIfEmpty(Mono.defer(() -> embeddingService.embed(text)
                        .flatMap(v -> embeddingCache.put(text, v).thenReturn(v))))
                .block(waitTimeout());
        return vector == null ? new float[0] : vector;
    }

    private Duration waitTimeout() {
        return Duration.ofMillis(props.getMemory().getEmbeddingTimeoutMs() + 2000);
    }

    private Long tenantId(RunContext ctx) {
        if (ctx == null || ctx.getTenantId() == null) {
            return null;
        }
        try {
            return Long.parseLong(ctx.getTenantId());
        } catch (NumberFormatException e) {
            log.warn("长期记忆工具租户 ID 非法 tenantId={}", ctx.getTenantId());
            return null;
        }
    }
}
