package cn.com.agentflow.quota;

import cn.com.agentflow.mapper.QuotaReservationMapper;
import cn.com.agentflow.mapper.QuotaReservationRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 配额账本：best-effort 写失败不抛错，正常路径返回账本 id */
class QuotaReservationLedgerTest {

    private QuotaReservationMapper mapper;
    private QuotaReservationLedger ledger;

    @BeforeEach
    void setUp() {
        mapper = mock(QuotaReservationMapper.class);
        ledger = new QuotaReservationLedger(mapper);
    }

    @Test
    void 登记INIT成功返回账本id() {
        when(mapper.insertInit(any())).thenAnswer(invocation -> {
            QuotaReservationRow row = invocation.getArgument(0);
            row.setId(42L);
            return 1;
        });

        StepVerifier.create(ledger.begin("run-1", "mock-large", 200000))
                .expectNext(42L)
                .verifyComplete();
    }

    @Test
    void 登记INIT失败只告警并返回空() {
        when(mapper.insertInit(any())).thenThrow(new RuntimeException("ledger db down"));

        StepVerifier.create(ledger.begin("run-1", "mock-large", 200000))
                .verifyComplete();
    }

    @Test
    void RESERVED迁移失败不影响调用方() {
        when(mapper.casReserved(anyLong(), anyInt())).thenThrow(new RuntimeException("db down"));

        StepVerifier.create(ledger.markReserved(7L, 300))
                .verifyComplete();
    }

    @Test
    void SETTLED迁移失败不影响调用方() {
        when(mapper.casSettled(anyLong(), anyInt())).thenThrow(new RuntimeException("db down"));

        StepVerifier.create(ledger.markSettled(7L, 120))
                .verifyComplete();
    }

    @Test
    void 无账本占位id不访问数据库() {
        StepVerifier.create(ledger.markReserved(QuotaReservationLedger.noLedger(), 300))
                .verifyComplete();
        StepVerifier.create(ledger.markSettled(QuotaReservationLedger.noLedger(), 120))
                .verifyComplete();

        verify(mapper, never()).casReserved(anyLong(), anyInt());
        verify(mapper, never()).casSettled(anyLong(), anyInt());
    }
}
