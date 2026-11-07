package cn.com.agentflow.memory;

import cn.com.agentflow.config.AgentFlowProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * 长期记忆（pgvector）。
 * 写入：content_hash 做 (tenant_id, content_hash) 级去重，重复内容 ON CONFLICT DO NOTHING。
 * 召回：余弦距离 <-> 排序取候选，再用 score = similarity * weight * exp(-lambda * 天数) 重排，
 * 低于相似度阈值的不召回。JDBC 阻塞调用统一切到 boundedElastic。
 */
@Service
public class VectorMemory {

    private final JdbcTemplate jdbc;
    private final AgentFlowProperties props;

    public VectorMemory(JdbcTemplate jdbc, AgentFlowProperties props) {
        this.jdbc = jdbc;
        this.props = props;
    }

    /** 召回结果：similarity 为余弦相似度，score 为加权时效衰减后的最终得分 */
    public record MemoryItem(long id, String content, double similarity, double score) {
    }

    /** 写入记忆；空向量（embedding 服务降级）直接跳过，避免写入 NULL 向量 */
    public Mono<Void> save(long tenantId, Long sessionId, String content, float[] embedding, float weight) {
        if (embedding == null || embedding.length == 0) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> jdbc.update(
                        "INSERT INTO t_memory(tenant_id, session_id, content, content_hash, embedding, weight) "
                                + "VALUES (?, ?, ?, ?, ?::vector, ?) "
                                + "ON CONFLICT (tenant_id, content_hash) DO NOTHING",
                        tenantId, sessionId, content, sha256Hex(content), toVectorLiteral(embedding), weight))
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /** 召回：先按余弦距离取 topK 候选，再按相似度阈值过滤、按时效衰减重排 */
    public Mono<List<MemoryItem>> recall(long tenantId, float[] queryEmbedding, Integer topK) {
        if (queryEmbedding == null || queryEmbedding.length == 0) {
            return Mono.just(List.of());
        }
        int limit = topK == null ? props.getMemory().getRecallTopK() : topK;
        String vector = toVectorLiteral(queryEmbedding);
        return Mono.fromCallable(() -> {
                    List<Map<String, Object>> rows = jdbc.queryForList(
                            "SELECT id, content, weight, "
                                    + "EXTRACT(EPOCH FROM (now() - created_at)) / 86400.0 AS age_days, "
                                    + "1 - (embedding <=> ?::vector) AS similarity "
                                    + "FROM t_memory "
                                    + "WHERE tenant_id = ? AND embedding IS NOT NULL "
                                    + "ORDER BY embedding <=> ?::vector "
                                    + "LIMIT ?",
                            vector, tenantId, vector, limit);
                    double threshold = props.getMemory().getRecallThreshold();
                    double lambda = props.getMemory().getWeightLambda();
                    List<MemoryItem> items = new ArrayList<>(rows.size());
                    for (Map<String, Object> row : rows) {
                        double similarity = ((Number) row.get("similarity")).doubleValue();
                        if (similarity < threshold) {
                            continue;
                        }
                        double weight = ((Number) row.get("weight")).doubleValue();
                        double ageDays = ((Number) row.get("age_days")).doubleValue();
                        double score = similarity * weight * Math.exp(-lambda * ageDays);
                        items.add(new MemoryItem(((Number) row.get("id")).longValue(),
                                (String) row.get("content"), similarity, score));
                    }
                    items.sort(Comparator.comparingDouble(MemoryItem::score).reversed());
                    return items;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /** float[] 转 pgvector 字面量：[0.1,0.2,...] */
    static String toVectorLiteral(float[] vector) {
        StringBuilder sb = new StringBuilder(vector.length * 8 + 2);
        sb.append('[');
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(vector[i]);
        }
        return sb.append(']').toString();
    }

    private static String sha256Hex(String raw) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(raw.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK 缺少 SHA-256 实现", e);
        }
    }
}
