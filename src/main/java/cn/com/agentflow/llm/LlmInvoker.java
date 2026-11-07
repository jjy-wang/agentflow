package cn.com.agentflow.llm;

import cn.com.agentflow.auth.TenantContext;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.exception.ErrorCodes;
import cn.com.agentflow.metric.AgentMetrics;
import cn.com.agentflow.quota.ModelBulkhead;
import cn.com.agentflow.quota.QuotaReservationLedger;
import cn.com.agentflow.quota.QuotaService;
import cn.com.agentflow.quota.TokenReservation;
import cn.com.agentflow.util.TokenEstimator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.util.context.ContextView;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LLM 调用入口：本地预扣 → 舱壁 → 流式调用 → 结算。
 *
 * <p>兜底状态机（对标 litellm fallbacks / cooldown）：</p>
 * <ol>
 *   <li>按 {@link ModelRouter.RoutePlan} 的候选链顺序尝试；金丝雀只决定首选，
 *       fallback 决定首选失败后选谁，两者不冲突；</li>
 *   <li>只有可重试错误（5xx / 超时 / 连接失败 / 429 / 舱壁繁忙 / 上下文超限）才切换下一个，
 *       业务错误（4xx 参数非法、内容违规）直接向上抛，不浪费其他模型的调用；</li>
 *   <li>上下文超限优先切到 {@code contextWindowFallbacks}，没有配置时才继续普通链；</li>
 *   <li>失败模型计入连续失败，达阈值后由 {@link ModelRouter} 打开熔断，冷却期内跳过；</li>
 *   <li>单次请求最多尝试 {@code max-attempts} 个模型，候选链已去重，切换不会成环。</li>
 * </ol>
 *
 * <p><b>计费口径：</b>每个候选模型各自预扣；只有最终成功的那一次调用按实际 token 结算，
 * 失败/取消的中间尝试按 0 结算并全额退回预扣。因此 fallback 多次调用不会对同一请求重复计费。</p>
 *
 * <p>本地预扣退款：预扣成功但调用失败/超时/取消时，必须归还本地令牌桶，
 * 否则桶会被抽干导致“配额耗尽被误报为没有可用模型”。退款通过 {@link LocalRefund}
 * 的原子标记保证一次预扣只退一次（doFinally 与 onErrorResume 双触发也安全）。</p>
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class LlmInvoker {

    /** 调用方通过 Reactor Context 透传的 runId 键；未透传时用调用关联 ID 兜底 */
    private static final String RUN_ID_KEY = "runId";

    /** 上下文超限的常见上游文案（大小写不敏感），用于把 4xx 细分出“换大窗口模型”分支 */
    private static final List<String> CONTEXT_OVERFLOW_HINTS = List.of(
            "context_length_exceeded", "context length", "maximum context length",
            "context window", "too many tokens", "prompt is too long", "reduce the length");

    private final ModelRouter router;
    private final QuotaService quota;
    private final ModelBulkhead bulkhead;
    private final AgentMetrics metrics;
    private final QuotaReservationLedger ledger;

    public Flux<ChatChunk> invoke(ChatRequest request) {
        // 租户随 Reactor Context 传递（无上下文时按无粘性处理），供金丝雀粘性路由使用
        return Flux.deferContextual(ctx -> {
            int estimatedPrompt = TokenEstimator.estimate(join(request.messages()));
            int estimatedTotal = estimatedPrompt + Math.max(0, request.maxTokens());
            ModelRouter.RoutePlan plan = router.route(request.model(), tenantOrNull(ctx), estimatedTotal);
            return tryFrom(plan, new RouteState(), 0, estimatedPrompt, request, runIdOf(ctx));
        });
    }

    private String tenantOrNull(ContextView ctx) {
        try {
            return TenantContext.reader().apply(ctx);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private String runIdOf(ContextView ctx) {
        return ctx.hasKey(RUN_ID_KEY)
                ? String.valueOf(ctx.get(RUN_ID_KEY))
                : "llm-" + UUID.randomUUID().toString().replace("-", "");
    }

    private Flux<ChatChunk> tryFrom(ModelRouter.RoutePlan plan, RouteState state, int idx,
                                    int estimatedPrompt, ChatRequest request, String runId) {
        List<ModelRouter.RouteTarget> chain = state.contextMode
                ? plan.contextWindowFallbacks() : plan.candidates();
        if (idx >= chain.size() || state.attempts >= plan.maxAttempts()) {
            return Flux.error(terminalError(state));
        }
        ModelRouter.RouteTarget target = chain.get(idx);
        boolean tried = state.tried.contains(target.modelName());
        boolean allowed = router.allowRequest(target.modelName());
        // 已尝试过（两条链切换时可能重复）或正在冷却：跳过且不消耗尝试次数
        if (tried || !allowed) {
            if (!tried && !allowed) {
                // 记录“被冷却跳过”，终态据此区分“确实没有模型”与“模型都在冷却”
                state.cooldownSkipped = true;
            }
            log.debug("跳过候选模型: model={}, tried={}, cooldown={}",
                    target.modelName(), tried, !allowed);
            return tryFrom(plan, state, idx + 1, estimatedPrompt, request, runId);
        }
        state.tried.add(target.modelName());
        int reserve = estimatedPrompt + Math.max(0, request.maxTokens());

        // 退款句柄提前创建：即使预扣完成前就被取消，后续 arm 也能把归还补齐
        LocalRefund refund = new LocalRefund(quota, metrics, target.modelName());
        return ledger.begin(runId, target.modelName(), target.config().getTpmLimit())
                .defaultIfEmpty(QuotaReservationLedger.noLedger())
                .flatMapMany(ledgerId -> Mono.defer(() -> quota.reserve(
                                target.modelName(), target.config().getRpmLimit(),
                                target.config().getTpmLimit(), reserve))
                        // 配额服务异常沿用 fail-open 语义，不能因限流组件故障阻断模型调用
                        .onErrorResume(e -> {
                            log.warn("配额预扣异常，降级放行: model={}, err={}",
                                    target.modelName(), e.toString());
                            return Mono.just(new TokenReservation(
                                    target.modelName(), reserve, true, "DEGRADED"));
                        })
                        .flatMapMany(res -> {
                            if (!res.granted()) {
                                // 准入失败没有扣减，绝不能退款；只记录拒绝来源用于终态错误码
                                state.quotaRejected = true;
                                if ("local".equals(quotaScope(res.reason()))) {
                                    state.rateLimited = true;
                                }
                                metrics.quotaRejected(target.modelName(), quotaScope(res.reason()));
                                // 配额拒绝不是模型故障，归还半开探测名额后换下一个模型
                                router.onAbort(target.modelName());
                                return tryFrom(plan, state, idx + 1, estimatedPrompt, request, runId);
                            }
                            refund.arm(res.reserved());
                            // 只统计“真正发生过调用”的模型切换；被配额/冷却拦下未调用的候选不算 fallback
                            if (state.lastModel != null && !state.lastModel.equals(target.modelName())) {
                                router.recordFallback(state.lastModel, target.modelName());
                            }
                            state.lastModel = target.modelName();
                            state.attempts++;
                            return ledger.markReserved(ledgerId, res.reserved())
                                    .thenMany(doInvoke(target, request, res, ledgerId, refund))
                                    // 流未启动（舱壁拒绝/账本异常）也要退款；流已启动时 doFinally 已退，
                                    // refundOnce 的原子标记保证同一次预扣只退一次
                                    .onErrorResume(e -> {
                                        refund.refundOnce();
                                        return handleFailure(plan, state, idx, target, e,
                                                estimatedPrompt, request, runId);
                                    });
                        }))
                // 兜底归还：覆盖“预扣成功但流还没订阅就被取消”的窄窗口。
                // 成功路径已在结算时按实际用量归还余量；refundOnce 幂等，失败/取消重复触发也安全。
                .doFinally(sig -> {
                    if (sig != SignalType.ON_COMPLETE) {
                        refund.refundOnce();
                    }
                });
    }

    private Flux<ChatChunk> doInvoke(ModelRouter.RouteTarget target, ChatRequest request,
                                     TokenReservation res, long ledgerId, LocalRefund refund) {
        // 自适应模式走 AIMD 舱壁，否则保持写死并发的原行为
        var permitMono = target.config().isAdaptiveConcurrency()
                ? bulkhead.acquireAdaptive(target.modelName(), target.config().getMaxConcurrency(),
                        target.config().getAdaptiveTtftThresholdMs())
                : bulkhead.acquire(target.modelName(), target.config().getMaxConcurrency());
        ChatRequest targetRequest = requestFor(target, request);
        return permitMono
                .flatMapMany(permit -> {
                    AtomicBoolean firstPacket = new AtomicBoolean(false);
                    AtomicInteger actual = new AtomicInteger(0);
                    AtomicLong ttftMs = new AtomicLong(-1);
                    long start = System.nanoTime();

                    return target.port().stream(targetRequest)
                            .doOnNext(chunk -> {
                                if (chunk instanceof ChatChunk.TextDelta
                                        && firstPacket.compareAndSet(false, true)) {
                                    ttftMs.set((System.nanoTime() - start) / 1_000_000);
                                    metrics.ttft(target.modelName(), start);
                                }
                                if (chunk instanceof ChatChunk.Usage u) {
                                    actual.set(u.promptTokens() + u.completionTokens());
                                    metrics.tokens(target.modelName(), u.promptTokens(), u.completionTokens());
                                }
                            })
                            // 首包超时单独控制：TTFT 差说明上游异常，比整体超时更早发现
                            .timeout(Duration.ofMillis(target.config().getTtftTimeoutMs()),
                                    Flux.error(new AgentException("TTFT_TIMEOUT", "首包超时")))
                            .timeout(Duration.ofMillis(target.config().getTimeoutMs()))
                            // 无论成功失败取消，都必须归还信号量、结算配额。
                            // 本地预检与 Redis 一样是“预扣-结算”：失败/取消全额归还，
                            // 成功只保留实际用量、把 maxTokens 预扣多出的部分退回。
                            .doFinally(sig -> {
                                if (sig == SignalType.ON_ERROR) {
                                    // 取消（如客户端断开）不算退化信号，只释放资源
                                    permit.failure();
                                    refund.refundOnce();
                                } else if (sig == SignalType.ON_COMPLETE) {
                                    permit.success(ttftMs.get());
                                    router.onSuccess(target.modelName());
                                } else {
                                    permit.close();
                                    refund.refundOnce();
                                    // 取消不是模型故障，归还半开探测名额
                                    router.onAbort(target.modelName());
                                }
                                int used = actual.get();
                                // 计费口径：只有真正成功的这一次调用按实际用量结算；
                                // 失败/取消的中间尝试一律按 0 结算（预扣全额退回），
                                // 因此 fallback 到多个模型也不会对同一请求重复计费。
                                int settled = sig == SignalType.ON_COMPLETE ? used : 0;
                                // 本地令牌桶同步“多退少补”：成功时只保留实际用量，
                                // 把 reserved - used 归还；失败路径已由 refundOnce 全额退回，避免重复退。
                                if (sig == SignalType.ON_COMPLETE) {
                                    int unused = Math.max(0, res.reserved() - used);
                                    if (unused > 0 && quota.refundLocal(target.modelName(), unused)) {
                                        metrics.quotaRefunded(target.modelName());
                                    }
                                }
                                // 只有结算成功才 CAS RESERVED → SETTLED；结算失败时账本停在 RESERVED，
                                // 由 QuotaReconciliationJob 补偿退款。账本自身写失败不影响结算与主链路。
                                quota.settle(target.modelName(), target.config().getTpmLimit(),
                                                res.reserved(), settled)
                                        .then(ledger.markSettled(ledgerId, settled))
                                        .onErrorResume(e -> {
                                            log.error("配额结算失败，账本保持 RESERVED 等待对账: "
                                                            + "model={}, reserved={}, used={}",
                                                    target.modelName(), res.reserved(), settled);
                                            return Mono.empty();
                                        })
                                        .subscribe();
                            });
                });
    }

    /** 失败分类后的状态迁移：业务错误直抛，其余决定换链还是换下一个模型 */
    private Flux<ChatChunk> handleFailure(ModelRouter.RoutePlan plan, RouteState state, int idx,
                                          ModelRouter.RouteTarget target, Throwable error,
                                          int estimatedPrompt, ChatRequest request, String runId) {
        FailureKind kind = classify(error);
        metrics.llmError(target.modelName(), error.getClass().getSimpleName());
        if (kind == FailureKind.BUSINESS) {
            // 参数非法/内容违规换模型也没用，直接向上抛，避免放大调用成本
            log.warn("模型业务错误，不触发兜底: model={}, err={}", target.modelName(), error.toString());
            return Flux.error(error);
        }
        state.lastError = error;
        if (kind == FailureKind.CONTEXT_WINDOW) {
            // 上下文超限：模型是健康的，优先换大窗口模型；没有配置时才继续普通链
            router.onAbort(target.modelName());
            if (!state.contextMode && !plan.contextWindowFallbacks().isEmpty()) {
                log.info("上下文超限，切换到大窗口兜底链: from={}, candidates={}",
                        target.modelName(), plan.contextWindowFallbacks().size());
                state.contextMode = true;
                return tryFrom(plan, state, 0, estimatedPrompt, request, runId);
            }
            return tryFrom(plan, state, idx + 1, estimatedPrompt, request, runId);
        }
        if (kind == FailureKind.BUSY) {
            // 舱壁拒绝说明模型繁忙而非故障，不计入熔断失败次数，终态也要报 MODEL_BUSY
            state.busyRejected = true;
            router.onAbort(target.modelName());
            return tryFrom(plan, state, idx + 1, estimatedPrompt, request, runId);
        }
        log.warn("模型调用失败，尝试下一个: model={}, err={}", target.modelName(), error.toString());
        router.onFailure(target.modelName());
        return tryFrom(plan, state, idx + 1, estimatedPrompt, request, runId);
    }

    /**
     * 终止错误：语义必须与真实原因一致，禁止把限流伪装成“没有可用模型”。
     *
     * <ol>
     *   <li>没有任何模型被真正调用时，按拒绝来源区分：
     *       本地预检拒绝 → {@code RATE_LIMITED}；Redis 配额拒绝 → {@code QUOTA_EXCEEDED}；
     *       舱壁满 → {@code MODEL_BUSY}；模型都在冷却 → {@code MODEL_BUSY}；
     *       路由确实找不到模型 → {@code NO_MODEL_AVAILABLE}；</li>
     *   <li>已经调用过模型但全部失败：优先透传最后一次的明确错误码，
     *       否则统一为 {@code UPSTREAM_ERROR}。</li>
     * </ol>
     */
    private Throwable terminalError(RouteState state) {
        if (state.attempts == 0) {
            if (state.rateLimited) {
                return new AgentException(ErrorCodes.RATE_LIMITED, "本地预检限流，所有候选模型均被拒绝");
            }
            if (state.quotaRejected) {
                return new AgentException(ErrorCodes.QUOTA_EXCEEDED, "所有候选模型配额不足");
            }
            if (state.busyRejected) {
                return new AgentException(ErrorCodes.MODEL_BUSY, "所有候选模型舱壁已满");
            }
            if (state.cooldownSkipped) {
                return new AgentException(ErrorCodes.MODEL_BUSY, "所有候选模型均处于冷却期");
            }
            return new AgentException(ErrorCodes.NO_MODEL_AVAILABLE, "路由未找到任何已注册模型");
        }
        if (state.busyRejected && state.lastError == null) {
            return new AgentException(ErrorCodes.MODEL_BUSY, "所有候选模型舱壁已满");
        }
        if (state.lastError instanceof AgentException ae) {
            return ae;
        }
        String detail = state.lastError == null ? "" : ": " + state.lastError.getMessage();
        return new AgentException(ErrorCodes.UPSTREAM_ERROR, "所有候选模型调用失败" + detail);
    }

    /** 兜底模型可能使用不同上游模型名，重建立请求避免把首选模型名发给备用供应商 */
    private ChatRequest requestFor(ModelRouter.RouteTarget target, ChatRequest request) {
        String upstream = target.config() != null && target.config().getModel() != null
                ? target.config().getModel() : target.modelName();
        if (upstream == null || upstream.equals(request.model())) {
            return request;
        }
        return new ChatRequest(upstream, request.messages(), request.tools(),
                request.maxTokens(), request.temperature());
    }

    private FailureKind classify(Throwable error) {
        Throwable t = unwrap(error);
        if (t instanceof WebClientResponseException w) {
            String body = safeBody(w);
            if (isContextOverflow(body) || isContextOverflow(w.getMessage())) {
                return FailureKind.CONTEXT_WINDOW;
            }
            int status = w.getStatusCode().value();
            // 5xx / 408 / 429 属于上游侧可重试错误；其余 4xx 是请求本身问题，不切换模型
            return (status >= 500 || status == 408 || status == 429)
                    ? FailureKind.RETRYABLE : FailureKind.BUSINESS;
        }
        if (t instanceof AgentException ae) {
            if (ErrorCodes.MODEL_BUSY.equals(ae.getCode())) {
                return FailureKind.BUSY;
            }
            return isContextOverflow(ae.getMessage()) ? FailureKind.CONTEXT_WINDOW : FailureKind.RETRYABLE;
        }
        if (isContextOverflow(t.getMessage())) {
            return FailureKind.CONTEXT_WINDOW;
        }
        if (t instanceof TimeoutException || t instanceof IOException) {
            return FailureKind.RETRYABLE;
        }
        // 未识别异常按可重试处理：宁可多试一个备用模型，也不要让一次抖动直接杀死 run
        return FailureKind.RETRYABLE;
    }

    private Throwable unwrap(Throwable error) {
        Throwable t = error;
        for (int i = 0; i < 5 && t.getCause() != null
                && !(t instanceof WebClientResponseException)
                && !(t instanceof AgentException); i++) {
            t = t.getCause();
        }
        return t;
    }

    private String safeBody(WebClientResponseException e) {
        try {
            return e.getResponseBodyAsString();
        } catch (RuntimeException ignore) {
            return null;
        }
    }

    private boolean isContextOverflow(String text) {
        if (text == null || text.isBlank()) {
            return false;
        }
        String lower = text.toLowerCase(Locale.ROOT);
        return CONTEXT_OVERFLOW_HINTS.stream().anyMatch(lower::contains);
    }

    /** 指标 scope 只允许 local / redis 两个固定枚举值 */
    private String quotaScope(String reason) {
        return reason != null && reason.startsWith(QuotaService.LOCAL_REJECT_PREFIX)
                ? "local" : "redis";
    }

    private String join(List<ChatMessage> messages) {
        StringBuilder sb = new StringBuilder();
        for (ChatMessage m : messages) {
            if (m.content() != null) {
                sb.append(m.content());
            }
        }
        return sb.toString();
    }

    private enum FailureKind {
        /** 5xx / 超时 / 连接失败 / 429：换下一个模型 */
        RETRYABLE,
        /** 上下文超限：优先换大窗口模型 */
        CONTEXT_WINDOW,
        /** 舱壁拒绝：换下一个模型但不算模型故障 */
        BUSY,
        /** 4xx 参数非法/内容违规：直接抛错，不兜底 */
        BUSINESS
    }

    /** 单次路由链的订阅级状态；每次 invoke 新建，彼此隔离 */
    private static final class RouteState {
        private final Set<String> tried = new HashSet<>();
        private int attempts;
        private boolean contextMode;
        /** 本次请求是否被拒绝过（本地/Redis 分别标记，用于终态错误码） */
        private boolean quotaRejected;
        private boolean rateLimited;
        /** 是否遇到过舱壁繁忙 */
        private boolean busyRejected;
        /** 是否有候选仅因冷却被跳过 */
        private boolean cooldownSkipped;
        /** 最近一次真正尝试的模型，用于统计 fallback 次数 */
        private String lastModel;
        private Throwable lastError;
    }

    /**
     * 本地预检退款句柄：一次预扣只退一次。
     * 原子标记同时屏蔽 doFinally 与 onErrorResume 的双触发，以及取消/超时的重复回调。
     *
     * <p>顺序安全：即使“先请求退款、后 arm 预扣量”（预扣回来前就被取消）也不会漏退——
     * refundOnce 只置请求标记，arm 看到标记后再执行退款；未 arm 成功（准入失败）时
     * permits=0，永不退款。</p>
     */
    private static final class LocalRefund {
        private final QuotaService quota;
        private final AgentMetrics metrics;
        private final String model;
        /** 是否已经请求过退款；仅表示“有终止信号”，不代表已执行 */
        private final AtomicBoolean refundRequested = new AtomicBoolean(false);
        /** 退款是否已真正执行，保证幂等 */
        private final AtomicBoolean done = new AtomicBoolean(false);
        private volatile int permits;

        private LocalRefund(QuotaService quota, AgentMetrics metrics, String model) {
            this.quota = quota;
            this.metrics = metrics;
            this.model = model;
        }

        private void arm(int permits) {
            this.permits = permits;
            if (refundRequested.get()) {
                doRefund();
            }
        }

        private void refundOnce() {
            refundRequested.set(true);
            doRefund();
        }

        private void doRefund() {
            int p = permits;
            // 尚未 arm 时不提前置 done，等 arm 后补退
            if (p <= 0 || !done.compareAndSet(false, true)) {
                return;
            }
            if (quota.refundLocal(model, p)) {
                metrics.quotaRefunded(model);
            }
        }
    }
}
