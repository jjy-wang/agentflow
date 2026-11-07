package cn.com.agentflow.audit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.List;

/**
 * run 审计：链式哈希落 t_run_audit。
 * 每行的 row_hash 包含同一 run 上一条的 row_hash（prev_hash），篡改任一历史行都会断链。
 * 审计是旁路不是主链路：落库失败只记 error，绝不阻断业务。
 */
@Slf4j
@Service
public class RunAuditService {

    /** detail 列宽 VARCHAR(512)，超长截断防止插入失败 */
    private static final int DETAIL_MAX = 512;

    private final JdbcTemplate jdbc;

    public RunAuditService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Mono<Void> record(String traceId, String tenantId, String runId, String action, String detail) {
        return Mono.fromRunnable(() -> {
                    List<String> prev = jdbc.queryForList(
                            "SELECT row_hash FROM t_run_audit WHERE run_id = ? ORDER BY id DESC LIMIT 1",
                            String.class, runId);
                    String prevHash = prev.isEmpty() ? null : prev.get(0);
                    // 规范化到 UTC + 微秒精度：PG timestamptz 只存微秒，
                    // 保证 AuditChainVerifier 读回 created_at 后能逐字节重建哈希输入
                    OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
                    String trimmed = detail == null ? null
                            : detail.length() <= DETAIL_MAX ? detail : detail.substring(0, DETAIL_MAX);
                    String rowHash = rowHash(traceId, tenantId, runId, action, trimmed, prevHash, now);
                    jdbc.update(
                            "INSERT INTO t_run_audit(trace_id, tenant_id, run_id, action, detail, prev_hash, row_hash, created_at) "
                                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            traceId, Long.parseLong(tenantId), runId, action, trimmed, prevHash, rowHash, now);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(e -> {
                    log.error("审计落库失败（不阻断业务）: runId={}, action={}, err={}", runId, action, e.toString());
                    return Mono.empty();
                })
                .then();
    }

    /**
     * 行哈希：sha256(traceId|tenantId|runId|action|detail|prevHash|createdAt)。
     * 与 AuditChainVerifier 共用同一算法，改动必须两侧同步。
     * createdAt 一律先规范化（UTC + 微秒）再取 toString，保证入库与读回重建结果一致。
     */
    static String rowHash(String traceId, String tenantId, String runId, String action,
                          String detail, String prevHash, OffsetDateTime createdAt) {
        OffsetDateTime canonical = createdAt.toInstant().atOffset(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.MICROS);
        return sha256Hex(String.join("|",
                nullToEmpty(traceId), nullToEmpty(tenantId), nullToEmpty(runId),
                nullToEmpty(action), nullToEmpty(detail),
                nullToEmpty(prevHash), canonical.toString()));
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
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
