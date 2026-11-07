package cn.com.agentflow.auth;

import cn.com.agentflow.exception.AgentException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * API Key 鉴权：库中只存 SHA-256 哈希，明文不落库。
 * 校验通过的租户结果本地缓存 60 秒，挡住高频请求的查库压力；
 * 缓存 key 用哈希而非明文，避免内存中留存敏感凭据。
 */
@Service
public class ApiKeyService {

    private static final Duration CACHE_TTL = Duration.ofSeconds(60);

    private final JdbcTemplate jdbc;
    private final Cache<String, String> cache;

    public ApiKeyService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.cache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(CACHE_TTL)
                .build();
    }

    /** 解析 API Key 所属租户；JDBC 为阻塞调用，必须切到 boundedElastic */
    public Mono<String> resolveTenant(String apiKey) {
        return Mono.fromCallable(() -> {
                    String keyHash = sha256Hex(apiKey);
                    String cached = cache.getIfPresent(keyHash);
                    if (cached != null) {
                        return cached;
                    }
                    List<Map<String, Object>> rows = jdbc.queryForList(
                            "SELECT tenant_id, status, expire_at FROM t_api_key WHERE key_hash = ?",
                            keyHash);
                    if (rows.isEmpty()) {
                        throw new AgentException("UNAUTHORIZED", "API Key 无效");
                    }
                    Map<String, Object> row = rows.get(0);
                    Number status = (Number) row.get("status");
                    Timestamp expireAt = (Timestamp) row.get("expire_at");
                    if (status == null || status.intValue() != 1) {
                        throw new AgentException("UNAUTHORIZED", "API Key 已禁用");
                    }
                    if (expireAt != null && expireAt.toInstant().isBefore(Instant.now())) {
                        throw new AgentException("UNAUTHORIZED", "API Key 已过期");
                    }
                    String tenantId = String.valueOf(row.get("tenant_id"));
                    cache.put(keyHash, tenantId);
                    return tenantId;
                })
                .subscribeOn(Schedulers.boundedElastic());
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
