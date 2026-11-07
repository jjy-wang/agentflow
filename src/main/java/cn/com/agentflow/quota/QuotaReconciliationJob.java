package cn.com.agentflow.quota;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.mapper.QuotaReservationMapper;
import cn.com.agentflow.mapper.QuotaReservationRow;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 配额预扣对账任务：扫描长时间停留在 INIT / RESERVED 的账本，做 Saga 补偿。
 *
 * <ul>
 *   <li>INIT：预扣尚未发生（或不确定），只把状态推成 REFUNDED，不退款；</li>
 *   <li>RESERVED：预扣已发生但结算缺失，用与 settle 相同的 Lua 脚本退回 reserved_tokens。</li>
 * </ul>
 *
 * <p>多实例/重复执行安全：先用 {@code UPDATE ... WHERE id=? AND status=?} CAS 抢占退款权，
 * 只有返回 1 的实例才执行退款，因此同一账本最多退款一次；抢不到直接跳过。
 * 不使用 SELECT FOR UPDATE，避免长事务持锁拖累主链路。</p>
 *
 * <p>指标只打固定名称与固定枚举标签（stage=SCAN/REFUND），runId/model 等高基数信息只进日志。</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class QuotaReconciliationJob {

    private static final String SCANNED = "quota.reconcile.scanned.total";
    private static final String REFUNDED = "quota.reconcile.refunded.total";
    private static final String SKIPPED = "quota.reconcile.skipped.total";
    private static final String FAILED = "quota.reconcile.failed.total";

    private final QuotaReservationMapper mapper;
    private final QuotaService quotaService;
    private final AgentFlowProperties props;
    private final MeterRegistry registry;

    @Scheduled(fixedDelayString = "${agentflow.quota.reconcile.interval-ms:60000}")
    public void reconcile() {
        AgentFlowProperties.Quota.Reconcile cfg = props.getQuota().getReconcile();
        if (!cfg.isEnabled()) {
            return;
        }

        List<QuotaReservationRow> stale;
        try {
            stale = mapper.findStale(cfg.getStaleAfter().toSeconds(), cfg.getBatchSize());
        } catch (Exception e) {
            log.error("配额对账扫描失败: err={}", e.toString());
            counter(FAILED, "SCAN").increment();
            return;
        }
        if (stale.isEmpty()) {
            return;
        }
        counter(SCANNED, null).increment(stale.size());

        int refunded = 0;
        int skipped = 0;
        for (QuotaReservationRow row : stale) {
            // 先记住扫描到的原状态：CAS 会把行改成 REFUNDED，后续判断仍按“进来时”的状态
            String fromStatus = row.getStatus();
            // CAS 抢占：抢不到说明已被其他实例/上一轮处理，跳过
            if (mapper.claimRefund(row.getId(), fromStatus) == 0) {
                skipped++;
                continue;
            }
            // INIT 阶段还没真正扣减，只改状态不退款；两点必须区分，否则会把没扣过的 token 白送出去
            if (!QuotaReservationStatus.RESERVED.name().equals(fromStatus)) {
                continue;
            }
            try {
                quotaService.refundReserved(row.getModel(), row.getTpmLimit(), row.getReservedTokens());
                refunded++;
            } catch (Exception e) {
                // 账本已是 REFUNDED（CAS 先行保证不重复退款）；退款未落地时留 ERROR 日志供人工补退
                log.error("配额对账退款失败（账本已置 REFUNDED，需人工补退）: id={}, runId={}, model={}, tokens={}, err={}",
                        row.getId(), row.getRunId(), row.getModel(), row.getReservedTokens(), e.toString());
                counter(FAILED, "REFUND").increment();
            }
        }

        if (refunded > 0) {
            counter(REFUNDED, null).increment(refunded);
        }
        if (skipped > 0) {
            counter(SKIPPED, null).increment(skipped);
        }
        log.info("配额对账完成: 扫描={}, 退款={}, CAS跳过={}", stale.size(), refunded, skipped);
    }

    /** stage 只允许 SCAN / REFUND 两个固定枚举值 */
    private Counter counter(String name, String stage) {
        Counter.Builder builder = Counter.builder(name);
        if (stage != null) {
            builder.tag("stage", stage);
        }
        return builder.register(registry);
    }
}
