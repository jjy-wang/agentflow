package cn.com.agentflow.audit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 审计链校验：完整链通过；篡改内容、断链、首行伪造均能检出并定位首个不匹配位置 */
class AuditChainVerifierTest {

    private JdbcTemplate jdbc;
    private AuditChainVerifier verifier;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        verifier = new AuditChainVerifier(jdbc);
    }

    private void stubRows(List<AuditChainVerifier.AuditRow> rows) {
        when(jdbc.query(anyString(), org.mockito.ArgumentMatchers.<RowMapper<AuditChainVerifier.AuditRow>>any(),
                eq("run-1"))).thenReturn(rows);
    }

    /** 构造一条哈希自洽的审计链 */
    private List<AuditChainVerifier.AuditRow> validChain(int n) {
        List<AuditChainVerifier.AuditRow> rows = new ArrayList<>();
        String prev = null;
        for (int i = 1; i <= n; i++) {
            OffsetDateTime ts = OffsetDateTime.now(ZoneOffset.UTC)
                    .truncatedTo(ChronoUnit.MICROS).plusSeconds(i);
            String hash = RunAuditService.rowHash("trace-1", "1", "run-1",
                    "TOOL_EXECUTED", "detail-" + i, prev, ts);
            rows.add(new AuditChainVerifier.AuditRow(i, "trace-1", 1L, "run-1",
                    "TOOL_EXECUTED", "detail-" + i, prev, hash, ts));
            prev = hash;
        }
        return rows;
    }

    @Test
    void 完整链校验通过() {
        stubRows(validChain(3));
        AuditChainVerifier.AuditChainReport report = verifier.verify("run-1").block();
        assertTrue(report.passed());
        assertEquals(3, report.total());
        assertEquals(-1, report.firstMismatchIndex());
        assertNull(report.firstMismatchId());
    }

    @Test
    void 篡改中间行detail被检出并定位() {
        List<AuditChainVerifier.AuditRow> rows = validChain(3);
        AuditChainVerifier.AuditRow tampered = rows.get(1);
        // 只改 detail 不改 row_hash：重算哈希必然不匹配
        rows.set(1, new AuditChainVerifier.AuditRow(tampered.id(), tampered.traceId(),
                tampered.tenantId(), tampered.runId(), tampered.action(), "被篡改的 detail",
                tampered.prevHash(), tampered.rowHash(), tampered.createdAt()));
        stubRows(rows);

        AuditChainVerifier.AuditChainReport report = verifier.verify("run-1").block();
        assertFalse(report.passed());
        assertEquals(1, report.firstMismatchIndex(), "第 2 条（下标 1）必须被定位");
        assertEquals(2L, report.firstMismatchId());
    }

    @Test
    void 链式引用断裂被检出() {
        List<AuditChainVerifier.AuditRow> rows = validChain(3);
        AuditChainVerifier.AuditRow broken = rows.get(2);
        // prev_hash 指向一个不存在的哈希：链断
        rows.set(2, new AuditChainVerifier.AuditRow(broken.id(), broken.traceId(),
                broken.tenantId(), broken.runId(), broken.action(), broken.detail(),
                "0000000000000000000000000000000000000000000000000000000000000000",
                broken.rowHash(), broken.createdAt()));
        stubRows(rows);

        AuditChainVerifier.AuditChainReport report = verifier.verify("run-1").block();
        assertFalse(report.passed());
        assertEquals(2, report.firstMismatchIndex());
        assertTrue(report.message().contains("链式引用断裂"));
    }

    @Test
    void 首行prevHash非空判伪造() {
        List<AuditChainVerifier.AuditRow> rows = validChain(2);
        AuditChainVerifier.AuditRow first = rows.get(0);
        rows.set(0, new AuditChainVerifier.AuditRow(first.id(), first.traceId(),
                first.tenantId(), first.runId(), first.action(), first.detail(),
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                first.rowHash(), first.createdAt()));
        stubRows(rows);

        AuditChainVerifier.AuditChainReport report = verifier.verify("run-1").block();
        assertFalse(report.passed());
        assertEquals(0, report.firstMismatchIndex());
    }

    @Test
    void 无审计记录判通过() {
        stubRows(List.of());
        AuditChainVerifier.AuditChainReport report = verifier.verify("run-1").block();
        assertTrue(report.passed());
        assertEquals(0, report.total());
    }
}
