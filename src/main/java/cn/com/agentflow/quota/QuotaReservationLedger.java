package cn.com.agentflow.quota;

import cn.com.agentflow.mapper.QuotaReservationMapper;
import cn.com.agentflow.mapper.QuotaReservationRow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * 配额预扣账本（Saga 补偿用）。
 *
 * <p>写账本一律 best-effort：失败只打 WARN 并继续主链路，把账本当作“补偿依据”而不是
 * “正确性底线”。理由：账本是一次额外的 DB 写，若把它变成硬依赖，DB 抖动会放大成
 * 整条 LLM 链路不可用；账本写失败的代价是少退/多退一次（见 DESIGN“配额 Saga 与对账补偿”），
 * 但不会让租户额度被永久吃掉，也不会阻断模型调用。</p>
 *
 * <p>所有 JDBC 写操作切到 boundedElastic，避免阻塞 WebFlux 事件循环。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class QuotaReservationLedger {

    /** 无账本时的占位 id：预扣/结算照常执行，仅跳过账本写入 */
    private static final long NO_LEDGER = -1L;

    private final QuotaReservationMapper mapper;

    /** 预扣前登记 INIT；返回账本 id，写失败返回空（调用方用 defaultIfEmpty 兜底） */
    public Mono<Long> begin(String runId, String model, int tpmLimit) {
        return Mono.fromCallable(() -> {
                    QuotaReservationRow row = new QuotaReservationRow();
                    row.setRunId(runId);
                    row.setModel(model);
                    row.setTpmLimit(tpmLimit);
                    int inserted = mapper.insertInit(row);
                    if (inserted <= 0 || row.getId() == null) {
                        log.warn("配额账本 INIT 未写入: runId={}, model={}", runId, model);
                        return null;
                    }
                    return row.getId();
                })
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(e -> log.warn("配额账本 INIT 写入失败（不阻断主链路）: runId={}, model={}, err={}",
                        runId, model, e.toString()))
                .onErrorResume(e -> Mono.empty());
    }

    /** 预扣成功后 CAS：INIT → RESERVED，并记录实际预扣量 */
    public Mono<Void> markReserved(long id, int reservedTokens) {
        if (id == NO_LEDGER) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> mapper.casReserved(id, reservedTokens))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(e -> log.warn("配额账本 RESERVED 迁移失败（不阻断主链路）: id={}, err={}",
                        id, e.toString()))
                .onErrorResume(e -> Mono.empty())
                .then();
    }

    /** 结算完成后 CAS：RESERVED → SETTLED */
    public Mono<Void> markSettled(long id, int actualTokens) {
        if (id == NO_LEDGER) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> mapper.casSettled(id, actualTokens))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(e -> log.warn("配额账本 SETTLED 迁移失败（不阻断主链路）: id={}, err={}",
                        id, e.toString()))
                .onErrorResume(e -> Mono.empty())
                .then();
    }

    /** 无账本占位 id，仅供 LlmInvoker 在账本写失败后继续主链路 */
    public static long noLedger() {
        return NO_LEDGER;
    }
}
