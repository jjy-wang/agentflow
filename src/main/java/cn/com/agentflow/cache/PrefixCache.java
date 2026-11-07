package cn.com.agentflow.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

/**
 * 前缀缓存：相同 (model, prefix) 的模型输出直接复用，省一次上游调用。
 * key 用 SHA-256 而非原文，避免 Redis key 里留存提示词内容。
 * Redis 故障时读写都降级为空，缓存 miss 走正常调用链。
 */
@Slf4j
@Component
public class PrefixCache {

    private static final String KEY_PREFIX = "cache:prefix:";

    private final ReactiveStringRedisTemplate redis;

    public PrefixCache(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    public Mono<String> get(String model, String prefix) {
        return redis.opsForValue().get(key(model, prefix))
                .onErrorResume(e -> {
                    log.warn("前缀缓存读取失败，按 miss 处理: err={}", e.toString());
                    return Mono.empty();
                });
    }

    public Mono<Void> put(String model, String prefix, String value, Duration ttl) {
        return redis.opsForValue().set(key(model, prefix), value, ttl)
                .then()
                .onErrorResume(e -> {
                    log.warn("前缀缓存写入失败，跳过: err={}", e.toString());
                    return Mono.empty();
                });
    }

    static String key(String model, String prefix) {
        return KEY_PREFIX + sha256Hex(model + ':' + prefix);
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
