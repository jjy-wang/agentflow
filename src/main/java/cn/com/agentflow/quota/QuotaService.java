package cn.com.agentflow.quota;

import cn.com.agentflow.config.AgentFlowProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class QuotaService {

    /** 本地预检拒绝原因前缀：调用方据此把指标 scope 标成 local，与 Redis 拒绝区分 */
    public static final String LOCAL_REJECT_PREFIX = "LOCAL_";
    /** 本地桶 key 前缀，扣减与退款必须使用同一 key */
    private static final String LOCAL_TPM_KEY_PREFIX = "tpm:";

    private static final RedisScript<List> RESERVE = RedisScript.of(
            load("lua/quota_reserve.lua"), List.class);
    private static final RedisScript<Long> SETTLE = RedisScript.of(
            load("lua/quota_settle.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final LocalRateLimiter local;
    private final AgentFlowProperties props;

    private static ClassPathResource load(String path) {
        return new ClassPathResource(path);
    }

    /**
     * 预扣：先确认请求数（RPM），再按 prompt + maxTokens 预扣 token（TPM）。
     * 时间源用 Redis TIME（Lua 内部），避免多实例时钟漂移。
     */
    @SuppressWarnings("unchecked")
    public Mono<TokenReservation> reserve(String model, int rpmLimit, int tpmLimit, int reserveTokens) {
        // 本地快速预检：明显超量直接拒绝，不打 Redis
        int localCapacity = (int) (tpmLimit * props.getQuota().getLocalFactor());
        if (props.getQuota().isLocalPrecheckEnabled()
                && !local.tryAcquire(LOCAL_TPM_KEY_PREFIX + model, model, localCapacity, reserveTokens)) {
            return Mono.just(TokenReservation.deny(model, "LOCAL_TPM_EXCEEDED"));
        }

        return Mono.fromCallable(() -> {
                    List<?> r = redis.execute(RESERVE,
                            List.of("quota:rpm:" + model, "quota:tpm:" + model),
                            String.valueOf(rpmLimit), String.valueOf(tpmLimit), String.valueOf(reserveTokens));
                    if (r == null || r.isEmpty()) {
                        return TokenReservation.deny(model, "QUOTA_SCRIPT_ERROR");
                    }
                    long granted = ((Number) r.get(0)).longValue();
                    String reason = String.valueOf(r.get(1));
                    return granted == 1
                            ? new TokenReservation(model, reserveTokens, true, "OK")
                            : TokenReservation.deny(model, reason);
                })
                .subscribeOn(Schedulers.boundedElastic())
                // Redis 不可用 → 降级放行（可用性优先于精确性），必须打日志与指标
                .onErrorResume(e -> {
                    log.warn("配额服务不可用，降级放行: model={}, err={}", model, e.toString());
                    return Mono.just(new TokenReservation(model, reserveTokens, true, "DEGRADED"));
                });
    }

    /**
     * 退还本地预检扣掉的令牌。与 {@link #reserve} 的本地扣减严格对称：
     * 同一 key 前缀、同一容量口径；未开启本地预检或额度非法时返回 false，
     * 调用方据此决定是否计退款指标（避免“退款成功但根本没扣过”的虚高计数）。
     *
     * <p>非阻塞（纯内存操作），无需切 boundedElastic。</p>
     */
    public boolean refundLocal(String model, int permits) {
        if (permits <= 0 || !props.getQuota().isLocalPrecheckEnabled()) {
            return false;
        }
        local.refund(LOCAL_TPM_KEY_PREFIX + model, permits);
        return true;
    }

    /** 结算：refund = 预扣 - 实际。异常路径也必须调用，否则配额永久减少 */
    public Mono<Void> settle(String model, int tpmLimit, int reserved, int actual) {
        int refund = Math.max(0, reserved - actual);
        if (refund == 0) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> redis.execute(SETTLE,
                        List.of("quota:tpm:" + model), String.valueOf(refund), String.valueOf(tpmLimit)))
                .subscribeOn(Schedulers.boundedElastic())
                // 错误向上抛：调用方据此决定是否把账本推进到 SETTLED（失败则留给对账补偿）
                .doOnError(e -> log.error("配额结算失败，等待对账补偿: model={}, refund={}", model, refund, e))
                .then();
    }

    /**
     * 对账补偿退款：把预扣成功但未走完结算的 token 退回桶。
     * 复用与 settle 完全相同的 Lua 脚本（同样的容量上限与 Redis TIME 时间源），
     * 同步阻塞执行，仅由对账任务线程调用；失败向上抛出，由任务记录 ERROR 与指标。
     */
    public void refundReserved(String model, int tpmLimit, int reservedTokens) {
        if (reservedTokens <= 0) {
            return;
        }
        redis.execute(SETTLE, List.of("quota:tpm:" + model),
                String.valueOf(reservedTokens), String.valueOf(tpmLimit));
        log.info("配额对账退款完成: model={}, tokens={}", model, reservedTokens);
    }
}
