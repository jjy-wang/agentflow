package cn.com.agentflow.quota;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.mapper.QuotaReservationMapper;
import cn.com.agentflow.mapper.QuotaReservationRow;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** 配额 Saga 对账：INIT 不退款、RESERVED 退一次、重复执行幂等、CAS 抢占失败跳过 */
class QuotaReconciliationJobTest {

    private FakeMapper mapper;
    private StringRedisTemplate redis;
    private SimpleMeterRegistry registry;
    private QuotaReconciliationJob job;

    @BeforeEach
    void setUp() {
        mapper = new FakeMapper();
        redis = mock(StringRedisTemplate.class);
        registry = new SimpleMeterRegistry();
        AgentFlowProperties props = new AgentFlowProperties();
        QuotaService quotaService = new QuotaService(redis, new LocalRateLimiter(registry), props);
        job = new QuotaReconciliationJob(mapper, quotaService, props, registry);
    }

    @Test
    void INIT超时_只改状态不退款() {
        long id = mapper.insertRow("run-init", "mock-large", 200000, 0, QuotaReservationStatus.INIT);

        job.reconcile();

        assertEquals(QuotaReservationStatus.REFUNDED.name(), mapper.rows.get(id).getStatus());
        verifyNoInteractions(redis);
    }

    @Test
    void RESERVED超时_按预扣量退款一次() {
        long id = mapper.insertRow("run-reserved", "mock-large", 200000, 300, QuotaReservationStatus.RESERVED);
        doReturn(1L).when(redis).execute(any(RedisScript.class), anyList(), anyString(), anyString());

        job.reconcile();

        assertEquals(QuotaReservationStatus.REFUNDED.name(), mapper.rows.get(id).getStatus());
        verify(redis, times(1)).execute(any(RedisScript.class),
                eq(List.of("quota:tpm:mock-large")), eq("300"), eq("200000"));
        assertEquals(1.0, registry.get("quota.reconcile.refunded.total").counter().count());
    }

    @Test
    void 重复执行不会重复退款() {
        mapper.insertRow("run-dup", "mock-large", 200000, 300, QuotaReservationStatus.RESERVED);
        doReturn(1L).when(redis).execute(any(RedisScript.class), anyList(), anyString(), anyString());

        job.reconcile();
        job.reconcile();

        verify(redis, times(1)).execute(any(RedisScript.class), anyList(), anyString(), anyString());
    }

    @Test
    void CAS抢占失败则跳过退款() {
        mapper.insertRow("run-lost", "mock-large", 200000, 300, QuotaReservationStatus.RESERVED);
        mapper.claimAlwaysLoses = true;

        job.reconcile();

        verifyNoInteractions(redis);
        assertEquals(1.0, registry.get("quota.reconcile.skipped.total").counter().count());
    }

    /** 内存版账本 Mapper：模拟 findStale 返回快照、CAS 只允许一次成功 */
    private static final class FakeMapper implements QuotaReservationMapper {

        final Map<Long, QuotaReservationRow> rows = new LinkedHashMap<>();
        long seq = 1;
        boolean claimAlwaysLoses;

        long insertRow(String runId, String model, int tpmLimit, int reserved, QuotaReservationStatus status) {
            QuotaReservationRow row = new QuotaReservationRow();
            row.setId(seq++);
            row.setRunId(runId);
            row.setModel(model);
            row.setTpmLimit(tpmLimit);
            row.setReservedTokens(reserved);
            row.setStatus(status.name());
            rows.put(row.getId(), row);
            return row.getId();
        }

        @Override
        public int insertInit(QuotaReservationRow row) {
            row.setId(seq++);
            row.setStatus(QuotaReservationStatus.INIT.name());
            row.setReservedTokens(0);
            rows.put(row.getId(), row);
            return 1;
        }

        @Override
        public int casReserved(long id, int reservedTokens) {
            QuotaReservationRow row = rows.get(id);
            if (row == null || !QuotaReservationStatus.INIT.name().equals(row.getStatus())) {
                return 0;
            }
            row.setStatus(QuotaReservationStatus.RESERVED.name());
            row.setReservedTokens(reservedTokens);
            return 1;
        }

        @Override
        public int casSettled(long id, int actualTokens) {
            QuotaReservationRow row = rows.get(id);
            if (row == null || !QuotaReservationStatus.RESERVED.name().equals(row.getStatus())) {
                return 0;
            }
            row.setStatus(QuotaReservationStatus.SETTLED.name());
            row.setActualTokens(actualTokens);
            return 1;
        }

        @Override
        public List<QuotaReservationRow> findStale(long staleSeconds, int batch) {
            List<QuotaReservationRow> result = new ArrayList<>();
            for (QuotaReservationRow row : rows.values()) {
                boolean nonTerminal = QuotaReservationStatus.INIT.name().equals(row.getStatus())
                        || QuotaReservationStatus.RESERVED.name().equals(row.getStatus());
                if (nonTerminal && result.size() < batch) {
                    result.add(copy(row));
                }
            }
            return result;
        }

        @Override
        public int claimRefund(long id, String expectedStatus) {
            if (claimAlwaysLoses) {
                return 0;
            }
            QuotaReservationRow row = rows.get(id);
            if (row == null || !expectedStatus.equals(row.getStatus())) {
                return 0;
            }
            row.setStatus(QuotaReservationStatus.REFUNDED.name());
            return 1;
        }

        private QuotaReservationRow copy(QuotaReservationRow source) {
            QuotaReservationRow target = new QuotaReservationRow();
            target.setId(source.getId());
            target.setRunId(source.getRunId());
            target.setModel(source.getModel());
            target.setTpmLimit(source.getTpmLimit());
            target.setReservedTokens(source.getReservedTokens());
            target.setActualTokens(source.getActualTokens());
            target.setStatus(source.getStatus());
            return target;
        }
    }
}
