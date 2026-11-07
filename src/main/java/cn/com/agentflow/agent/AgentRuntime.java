package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Agent 运行入口：加载系统提示词，按运行模式编排执行循环。
 * mode=REACT 走 ReActLoop；mode=PLAN_EXECUTE 走 PlanExecuteLoop。
 * run 级硬截止在这里兜底：单步预算再细，也需要一个总预算防止 8 步各慢一次拖到分钟级。
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class AgentRuntime {

    private static final String MODE_PLAN_EXECUTE = "PLAN_EXECUTE";
    private static final String FALLBACK_SYSTEM_PROMPT =
            "你是企业安全运营助手。必要时调用工具获取事实，不要编造数据。";

    private final ReActLoop reActLoop;
    private final PlanExecuteLoop planExecuteLoop;
    private final AgentFlowProperties props;

    /** 系统提示词缓存：进程内只需读一次类路径 */
    private volatile String cachedSystemPrompt;

    public Mono<String> execute(RunContext ctx) {
        return withBudget(ctx, runChain(ctx));
    }

    /**
     * 审批通过后续跑：不重发 run.started、不重复召回记忆，
     * 先补执行 state_json 中挂起的工具批次，再进入下一步 ReAct 循环。
     */
    public Mono<String> executeResume(RunContext ctx) {
        return withBudget(ctx, systemPrompt().flatMap(prompt -> reActLoop.resume(ctx, prompt)));
    }

    private Mono<String> withBudget(RunContext ctx, Mono<String> chain) {
        long budgetMs = maxDurationMs();
        return chain
                // 整条 run 的墙钟总预算：超时取消上游，LLM 层 doFinally 归还舱壁并把配额按已用量结算
                .timeout(Duration.ofMillis(budgetMs))
                .onErrorMap(TimeoutException.class, e -> new AgentException("RUN_TIMEOUT",
                        "run 总时长超过预算 " + budgetMs + "ms，已用 " + ctx.elapsedMs() + "ms"));
    }

    private Mono<String> runChain(RunContext ctx) {
        return systemPrompt().flatMap(prompt -> {
            if (MODE_PLAN_EXECUTE.equalsIgnoreCase(ctx.getMode())) {
                return planExecuteLoop.run(ctx, prompt);
            }
            return reActLoop.run(ctx, prompt);
        });
    }

    private long maxDurationMs() {
        return Math.max(1, props.getAgent().getMaxDurationMs());
    }

    /** 读取系统提示词；类路径读取是阻塞 IO，切到 boundedElastic */
    private Mono<String> systemPrompt() {
        String cached = cachedSystemPrompt;
        if (cached != null) {
            return Mono.just(cached);
        }
        return Mono.fromCallable(() -> {
                    try {
                        var res = new ClassPathResource("prompts/system_zh.txt");
                        String text = new String(res.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                        cachedSystemPrompt = text;
                        return text;
                    } catch (Exception e) {
                        log.warn("加载 prompts/system_zh.txt 失败，使用内置兜底提示词: {}", e.toString());
                        return FALLBACK_SYSTEM_PROMPT;
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}
