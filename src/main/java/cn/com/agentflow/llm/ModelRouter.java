package cn.com.agentflow.llm;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.metric.AgentMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 路由目标：候选按优先级返回，第一个失败可用第二个（兜底由 {@link LlmInvoker} 驱动）。
 * 支持金丝雀灰度：按权重把 from 模型的流量切到 to 模型，可按租户粘性分桶。
 *
 * <p>职责顺序（三者不冲突，按此顺序生效）：</p>
 * <ol>
 *   <li><b>金丝雀</b>决定“优先选谁”：只替换首选，不改变候选链的兜底语义；</li>
 *   <li><b>fallback 链</b>决定“首选挂了选谁”：首选可重试失败后按配置链依次尝试；</li>
 *   <li><b>熔断冷却</b>决定“谁暂时别选”：失败的模型进入冷却，冷却期内路由直接跳过。</li>
 * </ol>
 *
 * <p>模型兜底矩阵（对标 litellm）：</p>
 * <ul>
 *   <li>普通兜底：{@code fallback-chain} 有序链（兼容旧字段 {@code fallbacks}），
 *       模型级配置优先，未配置时使用 {@code agentflow.llm.fallback-chain} 全局链；</li>
 *   <li>上下文兜底：{@code context-window-fallbacks} 在同一条链之外单独返回，
 *       上下文超限时优先换大窗口模型；</li>
 *   <li>成本约束：{@code max-cost-per-request} + 每模型 {@code cost-per-1k-tokens}，
 *       超出预算的候选被过滤，全部超预算时保留最便宜的一个；</li>
 *   <li>冷却熔断：连续失败达阈值打开，冷却期内直接跳过，到期半开放少量探测。</li>
 * </ul>
 *
 * <p>熔断状态为实例内原子状态（多实例各自熔断）：成本最低、无额外网络往返；
 * 若需要跨实例强一致，可把 {@link CircuitState} 替换为 Redis 实现，接口不变。</p>
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class ModelRouter {

    private final ModelRegistry registry;
    private final AgentFlowProperties props;
    private final AgentMetrics metrics;
    private final MeterRegistry meterRegistry;

    /** 模型 → 熔断状态；只有出现失败的模型才会创建，正常模型无额外内存开销 */
    private final Map<String, CircuitState> circuits = new ConcurrentHashMap<>();

    public record RouteTarget(String modelName, ModelPort port, AgentFlowProperties.Model config) {
    }

    /**
     * 路由结果：普通候选链 + 上下文超限专用兜底链 + 单次请求最多尝试数。
     * 两条链各自去重，不会把同一个模型重复放进同一条链。
     */
    public record RoutePlan(List<RouteTarget> candidates,
                            List<RouteTarget> contextWindowFallbacks,
                            int maxAttempts) {
    }

    public List<RouteTarget> candidates(String requested) {
        return candidates(requested, null);
    }

    /** 旧入口：不带 token 估算，因此不做成本过滤 */
    public List<RouteTarget> candidates(String requested, String tenantId) {
        return route(requested, tenantId, 0).candidates();
    }

    /**
     * 构建候选链：首选（经金丝雀替换）→ 显式 fallback-chain → 兼容旧行为的隐式兜底。
     * 任何未注册/重复/超预算的模型都会被跳过；全部超预算时保留最便宜模型，避免拒绝服务。
     */
    public RoutePlan route(String requested, String tenantId, int estimatedTokens) {
        String primary = resolvePrimary(requested, tenantId);
        AgentFlowProperties.Model primaryConfig = configOf(primary);

        List<RouteTarget> candidates = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        addTarget(candidates, seen, primary, estimatedTokens);
        List<String> fallbackChain = fallbackChainOf(primaryConfig);
        for (String name : fallbackChain) {
            addTarget(candidates, seen, name, estimatedTokens);
        }
        // 显式配置了 fallback 链就以链为准；只有完全未配置时才沿用旧行为挂一个其他模型，
        // 避免在用户明确声明的链之外偷偷多调用一个供应商（成本与计费口径都不可控）
        if (fallbackChain.isEmpty()) {
            firstOther(primary).ifPresent(name -> addTarget(candidates, seen, name, estimatedTokens));
        }
        if (candidates.isEmpty()) {
            cheapestRegistered().ifPresent(candidates::add);
        }

        List<RouteTarget> contextChain = new ArrayList<>();
        Set<String> contextSeen = new LinkedHashSet<>();
        if (primaryConfig != null) {
            for (String name : primaryConfig.getContextWindowFallbacks()) {
                addTarget(contextChain, contextSeen, name, estimatedTokens);
            }
        }
        return new RoutePlan(List.copyOf(candidates), List.copyOf(contextChain),
                Math.max(1, props.getLlm().getMaxAttempts()));
    }

    /**
     * fallback 链优先级：模型级 {@code fallback-chain} > 旧字段 {@code fallbacks} >
     * 全局 {@code agentflow.llm.fallback-chain}。全部去重由 addTarget 负责。
     */
    private List<String> fallbackChainOf(AgentFlowProperties.Model primaryConfig) {
        if (primaryConfig != null) {
            if (primaryConfig.getFallbackChain() != null && !primaryConfig.getFallbackChain().isEmpty()) {
                return primaryConfig.getFallbackChain();
            }
            if (primaryConfig.getFallbacks() != null && !primaryConfig.getFallbacks().isEmpty()) {
                return primaryConfig.getFallbacks();
            }
        }
        List<String> globalChain = props.getLlm().getFallbackChain();
        return globalChain != null ? globalChain : List.of();
    }

    /** 命中灰度则把首选模型换成 to；from/to 未注册或权重为 0 时保持原路由 */
    private String applyCanary(String primary, String tenantId) {
        AgentFlowProperties.Canary c = props.getLlm().getCanary();
        if (!c.isEnabled() || c.getWeightPercent() <= 0
                || c.getFrom() == null || c.getTo() == null
                || !primary.equals(c.getFrom()) || !registry.contains(c.getTo())) {
            return primary;
        }
        if (bucket(tenantId, c.isStickyByTenant()) < c.getWeightPercent()) {
            metrics.canaryHit(c.getFrom(), c.getTo());
            return c.getTo();
        }
        return primary;
    }

    /** 粘性模式按租户哈希分桶（同租户多轮对话模型一致），否则每请求随机分桶 */
    private int bucket(String tenantId, boolean stickyByTenant) {
        if (stickyByTenant && tenantId != null) {
            return Math.floorMod(tenantId.hashCode(), 100);
        }
        return ThreadLocalRandom.current().nextInt(100);
    }

    private String resolvePrimary(String requested, String tenantId) {
        String primary = (requested != null && registry.contains(requested))
                ? requested : props.getLlm().getDefaultModel();
        return applyCanary(primary, tenantId);
    }

    private AgentFlowProperties.Model configOf(String name) {
        return name != null && registry.contains(name) ? registry.config(name) : null;
    }

    private boolean addTarget(List<RouteTarget> list, Set<String> seen, String name, int estimatedTokens) {
        if (name == null || name.isBlank() || !registry.contains(name) || seen.contains(name)) {
            return false;
        }
        AgentFlowProperties.Model cfg = registry.config(name);
        if (cfg == null || overBudget(cfg, estimatedTokens)) {
            return false;
        }
        seen.add(name);
        list.add(new RouteTarget(name, registry.port(name), cfg));
        return true;
    }

    private boolean overBudget(AgentFlowProperties.Model cfg, int estimatedTokens) {
        double budget = props.getLlm().getMaxCostPerRequest();
        return budget > 0 && estimatedTokens > 0 && cfg.getCostPer1kTokens() > 0
                && estimatedTokens / 1000.0 * cfg.getCostPer1kTokens() > budget;
    }

    private Optional<String> firstOther(String primary) {
        return props.getLlm().getModels().keySet().stream()
                .filter(name -> !name.equals(primary) && registry.contains(name))
                .findFirst();
    }

    private Optional<RouteTarget> cheapestRegistered() {
        return props.getLlm().getModels().entrySet().stream()
                .filter(e -> registry.contains(e.getKey()))
                .min(Comparator.comparingDouble(e -> e.getValue().getCostPer1kTokens()))
                .map(e -> new RouteTarget(e.getKey(), registry.port(e.getKey()), e.getValue()));
    }

    // ---------------------------------------------------------------------
    // 冷却熔断：CLOSED（正常）→ OPEN（冷却期内跳过）→ HALF_OPEN（限量探测）
    // ---------------------------------------------------------------------

    /**
     * 准入判断。三态语义：
     * <ul>
     *   <li>CLOSED：正常放行；</li>
     *   <li>OPEN 且冷却未到期：直接跳过该模型，走 fallback 链；</li>
     *   <li>OPEN 且冷却已到期：切到 HALF_OPEN，只放行 {@code half-open-probes} 个
     *       探测请求（默认 1 个），其余请求继续跳过；探测成功由
     *       {@link #onSuccess} 闭合，失败由 {@link #onFailure} 重新冷却。</li>
     * </ul>
     */
    public boolean allowRequest(String model) {
        CircuitState s = circuits.get(model);
        if (s == null) {
            return true;
        }
        synchronized (s) {
            long now = System.currentTimeMillis();
            if (s.phase == CircuitPhase.OPEN) {
                if (now < s.openUntilMs) {
                    return false;
                }
                // 冷却到期：进入半开，清零探测计数后重新发放名额
                s.phase = CircuitPhase.HALF_OPEN;
                s.probes = 0;
                log.debug("模型冷却结束，进入半开探测: model={}", model);
            }
            if (s.phase == CircuitPhase.HALF_OPEN) {
                int maxProbes = Math.max(1, props.getLlm().getCooldown().getHalfOpenProbes());
                if (s.probes < maxProbes) {
                    s.probes++;
                    log.debug("模型半开探测放行: model={}, probe={}/{}", model, s.probes, maxProbes);
                    return true;
                }
                return false;
            }
            return true;
        }
    }

    /**
     * 模型调用成功：清零连续失败并闭合熔断。
     * 半开探测成功也走这里，状态从 HALF_OPEN 回到 CLOSED。
     */
    public void onSuccess(String model) {
        CircuitState s = circuits.get(model);
        if (s == null) {
            return;
        }
        synchronized (s) {
            s.failures = 0;
            if (s.phase != CircuitPhase.CLOSED) {
                log.info("模型熔断闭合: model={}, from={}", model, s.phase);
                s.phase = CircuitPhase.CLOSED;
                s.openUntilMs = 0;
                s.probes = 0;
            }
        }
    }

    /**
     * 模型调用失败（仅可重试类错误计入）：连续失败达到阈值打开熔断；
     * 半开探测失败则重新打开并重新计时冷却。
     */
    public void onFailure(String model) {
        CircuitState s = circuits.computeIfAbsent(model, k -> new CircuitState());
        synchronized (s) {
            long now = System.currentTimeMillis();
            if (s.phase != CircuitPhase.CLOSED) {
                // 半开探测失败：立即重新冷却；该模型的探测名额清零
                s.phase = CircuitPhase.OPEN;
                s.openUntilMs = now + cooldownMsOf(model);
                s.probes = 0;
                countCooldown(model);
                log.warn("模型半开探测失败，重新熔断: model={}, cooldownMs={}",
                        model, cooldownMsOf(model));
                return;
            }
            s.failures++;
            if (s.failures >= Math.max(1, props.getLlm().getCooldown().getFailureThreshold())) {
                s.phase = CircuitPhase.OPEN;
                s.openUntilMs = now + cooldownMsOf(model);
                s.probes = 0;
                countCooldown(model);
                log.warn("模型连续失败 {} 次，打开熔断: model={}, cooldownMs={}",
                        s.failures, model, cooldownMsOf(model));
            }
        }
    }

    /**
     * 非模型故障的终止（取消、业务 4xx、上下文超限、配额拒绝、舱壁拒绝）：
     * 归还半开探测名额，但不累计失败次数——这些错误不代表上游模型不健康。
     * CLOSED 状态下 probes 恒为 0，调用天然无副作用。
     */
    public void onAbort(String model) {
        CircuitState s = circuits.get(model);
        if (s == null) {
            return;
        }
        synchronized (s) {
            if (s.phase == CircuitPhase.HALF_OPEN && s.probes > 0) {
                s.probes--;
            }
        }
    }

    /** fallback 指标：{@code from} 失败后切到 {@code to}，标签只放模型名（固定枚举） */
    public void recordFallback(String from, String to) {
        if (from == null || to == null || from.equals(to)) {
            return;
        }
        Counter.builder("llm.model.fallback.total")
                .tag("from", from)
                .tag("to", to)
                .register(meterRegistry)
                .increment();
    }

    private void countCooldown(String model) {
        Counter.builder("llm.model.cooldown.total")
                .tag("model", model)
                .register(meterRegistry)
                .increment();
    }

    /** 冷却时长：模型级 cooldown-ms 优先，未配置时用全局 open-ms */
    private long cooldownMsOf(String model) {
        AgentFlowProperties.Model cfg = configOf(model);
        if (cfg != null && cfg.getCooldownMs() != null && cfg.getCooldownMs() > 0) {
            return cfg.getCooldownMs();
        }
        return Math.max(1, props.getLlm().getCooldown().getOpenMs());
    }

    /** 熔断三态：CLOSED 正常 / OPEN 冷却中 / HALF_OPEN 半开探测 */
    private enum CircuitPhase {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private static final class CircuitState {
        /** 当前熔断阶段 */
        CircuitPhase phase = CircuitPhase.CLOSED;
        /** 连续失败次数（成功即清零） */
        int failures;
        /** 冷却截止时间；仅 OPEN 阶段有意义 */
        long openUntilMs;
        /** 当前半开窗口已放行的探测数 */
        int probes;
    }
}
