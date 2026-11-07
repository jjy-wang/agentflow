package cn.com.agentflow.audit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;

/**
 * 审计链完整性校验工具（内部工具类，不暴露 REST 端点）。
 * 给定 runId，按 id（插入顺序，即 created_at 顺序）读出 t_run_audit 全部记录，
 * 逐条做两项校验：
 *   1) 链式引用：本行 prev_hash 必须等于上一行的 row_hash（首行 prev_hash 为 null）；
 *   2) 内容哈希：用行内字段重算 row_hash，必须与库存值一致。
 * 任一项不匹配即判定该位置被篡改，报告首个不匹配位置。
 * 阻塞 JDBC 切 boundedElastic，与 RunAuditService 同一纪律。
 */
@Slf4j
public class AuditChainVerifier {

    private static final String SELECT_SQL =
            "SELECT id, trace_id, tenant_id, run_id, action, detail, prev_hash, row_hash, created_at "
                    + "FROM t_run_audit WHERE run_id = ? ORDER BY id ASC";

    private final JdbcTemplate jdbc;

    public AuditChainVerifier(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 校验报告：总数、首个不匹配位置（0 起，-1 表示无）、对应行 id、是否通过、结论描述 */
    public record AuditChainReport(String runId, int total, int firstMismatchIndex,
                                   Long firstMismatchId, boolean passed, String message) {
    }

    /** 单条审计行（仅校验所需字段） */
    record AuditRow(long id, String traceId, long tenantId, String runId, String action,
                    String detail, String prevHash, String rowHash, OffsetDateTime createdAt) {
    }

    public Mono<AuditChainReport> verify(String runId) {
        return Mono.fromCallable(() -> doVerify(runId))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private AuditChainReport doVerify(String runId) {
        List<AuditRow> rows = jdbc.query(SELECT_SQL, (rs, n) -> new AuditRow(
                rs.getLong("id"),
                rs.getString("trace_id"),
                rs.getLong("tenant_id"),
                rs.getString("run_id"),
                rs.getString("action"),
                rs.getString("detail"),
                rs.getString("prev_hash"),
                rs.getString("row_hash"),
                rs.getObject("created_at", OffsetDateTime.class)), runId);

        String prev = null;
        for (int i = 0; i < rows.size(); i++) {
            AuditRow row = rows.get(i);
            if (!Objects.equals(row.prevHash(), prev)) {
                return fail(runId, rows.size(), i, row.id(),
                        "链式引用断裂：prev_hash 与上一行 row_hash 不一致");
            }
            String recomputed = RunAuditService.rowHash(row.traceId(), String.valueOf(row.tenantId()),
                    row.runId(), row.action(), row.detail(), row.prevHash(), row.createdAt());
            if (!recomputed.equals(row.rowHash())) {
                return fail(runId, rows.size(), i, row.id(),
                        "内容哈希不匹配：行字段重算结果与 row_hash 不一致");
            }
            prev = row.rowHash();
        }
        log.info("审计链校验通过 runId={} 共 {} 条", runId, rows.size());
        return new AuditChainReport(runId, rows.size(), -1, null, true,
                "校验通过，共 " + rows.size() + " 条记录，链式哈希完整");
    }

    private AuditChainReport fail(String runId, int total, int index, long rowId, String reason) {
        log.warn("审计链校验失败 runId={} 第 {} 条（id={}）: {}", runId, index + 1, rowId, reason);
        return new AuditChainReport(runId, total, index, rowId, false,
                "第 " + (index + 1) + " 条记录（id=" + rowId + "）" + reason + "，疑似篡改");
    }
}
