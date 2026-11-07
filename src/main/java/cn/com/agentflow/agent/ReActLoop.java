package cn.com.agentflow.agent;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ChatChunk;
import cn.com.agentflow.llm.ChatMessage;
import cn.com.agentflow.llm.ChatRequest;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.memory.MemoryRetrievalService;
import cn.com.agentflow.stream.EventType;
import cn.com.agentflow.telemetry.GenAiSpan;
import cn.com.agentflow.telemetry.GenAiTelemetry;
import cn.com.agentflow.tool.ToolExecutor;
import cn.com.agentflow.tool.ToolRegistry;
import cn.com.agentflow.tool.ToolResult;
import cn.com.agentflow.llm.LlmInvoker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
@Component
public class ReActLoop {

    /** 单 run 结构化输出协议校验失败的最大回灌重试次数，超过即判失败 */
    private static final int MAX_PROTOCOL_RETRIES = 2;

    /** 单步超时后的最大重试次数：连续两次超时才判定 run 失败 */
    private static final int MAX_STEP_TIMEOUT_RETRIES = 1;

    private final LlmInvoker llmInvoker;
    private final ToolRegistry toolRegistry;
    private final ToolExecutor toolExecutor;
    private final ContextBuilder contextBuilder;
    private final ContextCompressor contextCompressor;
    private final LoopGuard loopGuard;
    private final OutputProtocolGuard outputProtocolGuard;
    private final AgentFlowProperties props;
    private final MemoryRetrievalService memoryRetrievalService;
    private final GenAiTelemetry telemetry;

    public Mono<String> run(RunContext ctx, String systemPrompt) {
        ctx.emit(EventType.RUN_STARTED, "{\"runId\":\"" + ctx.getRunId()
                + "\",\"model\":\"" + ctx.getModel() + "\",\"mode\":\"" + ctx.getMode() + "\"}");
        // 开跑前做一次长期记忆预检索；失败降级为无记忆，不阻断 run
        return withRunSpan(ctx, runSpan -> withMemory(ctx, systemPrompt)
                .flatMap(prompt -> loop(ctx, prompt)));
    }

    /**
     * 审批通过后续跑：先补执行挂起时持久化的工具调用批次，再把结果回灌上下文进入下一步。
     * 只补执行待审批批次，之前步骤已成功的工具不会再跑（历史消息里已有结果，
     * 且同一 toolCallId 仍受 ToolInvocationStore 唯一约束保护）。
     * 续跑不重发 run.started，避免客户端重复初始化。
     */
    public Mono<String> resume(RunContext ctx, String systemPrompt) {
        List<ToolCall> pending = List.copyOf(ctx.getPendingToolCalls());
        if (pending.isEmpty()) {
            return withRunSpan(ctx, runSpan -> loop(ctx, systemPrompt));
        }
        // 补执行前视为已获批，避免 precheck 再次挂起
        pending.forEach(call -> ctx.markCallApproved(call.id()));
        return withRunSpan(ctx, runSpan -> executeTools(ctx, pending, runSpan)
                .flatMap(outcome -> {
                    if (outcome.suspended()) {
                        return Mono.empty();
                    }
                    ctx.clearPendingToolCalls();
                    ctx.clearSuspended();
                    return loop(ctx, systemPrompt);
                }));
    }

    /**
     * run 根 span 边界：只做观测包装，不改变原有事件/控制流。
     * 根 span 写入 Reactor Context 供下游 step/tool 读取，跨度结束状态按完成/异常/取消收敛。
     */
    private Mono<String> withRunSpan(RunContext ctx, Function<GenAiSpan, Mono<String>> body) {
        return Mono.defer(() -> {
            GenAiSpan runSpan = telemetry.beginRun(ctx.getModel(), ctx.getMode());
            return body.apply(runSpan)
                    .contextWrite(context -> context.put(GenAiTelemetry.RUN_SPAN_KEY, runSpan))
                    .doOnError(runSpan::recordError)
                    .doFinally(signal -> {
                        runSpan.finish(runState(signal));
                        runSpan.end();
                    });
        });
    }

    private String runState(SignalType signal) {
        return switch (signal) {
            case ON_COMPLETE -> "COMPLETED";
            case ON_ERROR -> "ERROR";
            case CANCEL -> "CANCELLED";
            default -> "UNKNOWN";
        };
    }

    private Mono<String> withMemory(RunContext ctx, String systemPrompt) {
        return memoryRetrievalService.recallSection(ctx.getTenantId(), lastUserQuery(ctx))
                .map(section -> section == null || section.isBlank() ? systemPrompt : systemPrompt + section)
                .onErrorResume(e -> {
                    log.warn("长期记忆预检索异常，降级为无记忆 runId={} err={}", ctx.getRunId(), e.toString());
                    return Mono.just(systemPrompt);
                });
    }

    /** 本轮问题取最后一条 user 消息；无用户消息则不召回 */
    private String lastUserQuery(RunContext ctx) {
        List<ChatMessage> messages = ctx.getMessages();
        for (int i = messages.size() - 1; i >= 0; i--) {
            ChatMessage message = messages.get(i);
            if ("user".equals(message.role()) && message.content() != null) {
                return message.content();
            }
        }
        return null;
    }

    /**
     * 单步 = 一次 LLM 调用 + 该轮工具执行；单步预算只约束本步，不包含下一步递归，
     * 否则会退化成整条 run 只有一个超时。
     *
     * 超时语义：单步超时按可重试失败处理，最多重试 1 次；重试走本方法入口，
     * nextStep() 正常计数，因此不能绕过 LoopGuard 的步数上限；
     * 连续两次单步超时或 run 总预算（max-duration-ms）耗尽，才以 STEP_TIMEOUT / RUN_TIMEOUT
     * 终结整条 run，由 RunService 统一收尾（关闭 SSE、释放租约、按已用量结算配额）。
     * 取舍：重试会重放本步的 LLM 与工具调用，因此工具必须幂等（ToolExecutor 已按
     * (run_id, tool_call_id) 去重）；换来的是偶发抖动不至于直接把整个 run 判死。
     */
    private Mono<String> loop(RunContext ctx, String systemPrompt) {
        return Mono.defer(() -> {
            // ① 四道闸
            var violation = loopGuard.check(ctx);
            if (violation.isPresent()) {
                return Mono.error(new AgentException(violation.get().code(), violation.get().message()));
            }

            ctx.nextStep();
            ctx.emit(EventType.STEP_STARTED, "{\"step\":" + ctx.getStep() + ",\"kind\":\"LLM\"}");

            return step(ctx, systemPrompt)
                    // 只有本步真正完成（未超时）才打断「连续超时」计数
                    .doOnNext(outcome -> ctx.resetConsecutiveStepTimeouts())
                    .timeout(Duration.ofMillis(stepTimeoutMs()))
                    .onErrorResume(TimeoutException.class, e -> onStepTimeout(ctx))
                    .flatMap(outcome -> {
                        // 审批挂起：以正常完成信号结束本轮，不判失败、不覆盖 WAITING_APPROVAL
                        if (outcome.suspended()) {
                            return Mono.empty();
                        }
                        return outcome.finished()
                                ? Mono.just(outcome.text())
                                : Mono.defer(() -> loop(ctx, systemPrompt));
                    });
        });
    }

    /** 执行一步的 IO：压缩历史 → 构建请求 → 流式调用 → 协议校验 → 工具执行 */
    private Mono<StepOutcome> step(RunContext ctx, String systemPrompt) {
        // ② 历史超阈值先压缩，再构建请求
        return contextCompressor.maybeCompress(ctx)
                .then(Mono.deferContextual(view -> {
                    // 每次 LLM 调用一个 chat span，父 span 来自 Reactor Context 中的 run 根 span
                    GenAiSpan chat = telemetry.beginChat(GenAiTelemetry.currentRun(view), ctx.getModel());
                    return Mono.fromCallable(() -> contextBuilder.build(
                                    ctx, toolRegistry.definitions(ctx.getTenantId()), systemPrompt))
                            // ③ 流式调用：边推 SSE 边聚合
                            .flatMapMany(req -> {
                                chat.recordRequest(req);
                                return streamTurn(ctx, req);
                            })
                            .collectList()
                            .map(LlmTurn::merge)
                            .doOnNext(turn -> chat.recordResponse(turn.text(), turn.finishReason(),
                                    turn.promptTokens(), turn.completionTokens()))
                            .doOnError(chat::recordError)
                            .flatMap(turn -> {
                                ctx.addTokenUsed(turn.promptTokens() + turn.completionTokens());
                                ctx.addPromptTokens(turn.promptTokens());

                                // ④ 结构化输出协议校验：非法输出不直接判死，回灌格式反馈重试
                                var protocolViolation = outputProtocolGuard.validate(turn);
                                if (protocolViolation.isPresent()) {
                                    return protocolRetry(ctx, turn, protocolViolation.get());
                                }

                                ctx.append(ChatMessage.assistant(turn.text(), turn.toolCalls()));
                                if (!turn.hasToolCalls()) {
                                    return Mono.just(StepOutcome.done(turn.text()));
                                }
                                // ⑤ 执行工具 → 结果回灌；下一步由 loop() 递归发起；命中审批则挂起本步
                                return executeTools(ctx, turn.toolCalls(), chat);
                            })
                            // chat span 包住本步工具子 span，保证父子时长关系自洽
                            .doOnError(chat::recordError)
                            .doFinally(signal -> chat.end());
                }));
    }

    /**
     * 单步超时处理：第一次发 STEP_TIMEOUT_RETRY 并让 loop() 重跑本步（正常占用步数预算）；
     * 总预算耗尽按 RUN_TIMEOUT、连续两次超时按 STEP_TIMEOUT 终结 run。
     */
    private Mono<StepOutcome> onStepTimeout(RunContext ctx) {
        int consecutive = ctx.incConsecutiveStepTimeouts();
        long elapsed = ctx.elapsedMs();
        long stepBudget = stepTimeoutMs();
        if (elapsed >= maxDurationMs()) {
            return Mono.error(new AgentException("RUN_TIMEOUT",
                    "run 总时长超过预算 " + maxDurationMs() + "ms，已用 " + elapsed + "ms"));
        }
        if (consecutive > MAX_STEP_TIMEOUT_RETRIES) {
            log.warn("单步连续超时，run 失败 runId={} step={} consecutive={} elapsed={}ms",
                    ctx.getRunId(), ctx.getStep(), consecutive, elapsed);
            return Mono.error(new AgentException("STEP_TIMEOUT",
                    "单步连续 " + consecutive + " 次超过预算 " + stepBudget + "ms，已用 " + elapsed + "ms"));
        }
        // 事件类型复用现有 step.started，以 kind=STEP_TIMEOUT_RETRY 标识一次超时重试
        ctx.emit(EventType.STEP_STARTED, "{\"step\":" + ctx.getStep()
                + ",\"kind\":\"STEP_TIMEOUT_RETRY\",\"retry\":" + consecutive + "}");
        log.warn("单步超时，重试一步 runId={} step={} budget={}ms elapsed={}ms",
                ctx.getRunId(), ctx.getStep(), stepBudget, elapsed);
        return Mono.just(StepOutcome.next());
    }

    private long stepTimeoutMs() {
        return Math.max(1, props.getAgent().getStepTimeoutMs());
    }

    private long maxDurationMs() {
        return Math.max(1, props.getAgent().getMaxDurationMs());
    }

    /**
     * 协议违规回灌重试：非法输出仅以纯文本留档（不落 tool_calls，避免污染上下文配对），
     * 把"格式错误 + 协议说明"作为系统反馈回灌后返回 next()，由 loop() 发起下一步。
     * 重试走 loop() 入口，nextStep() 正常计数，回灌次数天然计入 LoopGuard 步数预算；
     * 单 run 回灌超过 MAX_PROTOCOL_RETRIES 次才以 PROTOCOL_INVALID 判失败。
     */
    private Mono<StepOutcome> protocolRetry(RunContext ctx, LlmTurn turn, String violation) {
        ctx.append(ChatMessage.assistant(turn.text(), null));
        if (ctx.getProtocolRetries() >= MAX_PROTOCOL_RETRIES) {
            log.warn("结构化输出回灌重试超限 runId={} retries={} violation={}",
                    ctx.getRunId(), ctx.getProtocolRetries(), violation);
            return Mono.error(new AgentException("PROTOCOL_INVALID",
                    "模型连续 " + (ctx.getProtocolRetries() + 1) + " 轮输出不符合协议: " + violation));
        }
        ctx.incProtocolRetry();
        // 事件类型复用最贴近的 step.started，以 kind=PROTOCOL_RETRY 标识一次回灌重试
        ctx.emit(EventType.STEP_STARTED, "{\"step\":" + ctx.getStep()
                + ",\"kind\":\"PROTOCOL_RETRY\",\"retry\":" + ctx.getProtocolRetries() + "}");
        ctx.append(ChatMessage.system(outputProtocolGuard.feedback(violation)));
        log.warn("结构化输出协议违规，回灌重试 runId={} retry={} violation={}",
                ctx.getRunId(), ctx.getProtocolRetries(), violation);
        return Mono.just(StepOutcome.next());
    }

    private Flux<ChatChunk> streamTurn(RunContext ctx, ChatRequest req) {
        return llmInvoker.invoke(req)
                .doOnNext(chunk -> {
                    if (chunk instanceof ChatChunk.TextDelta t) {
                        ctx.emit(EventType.LLM_DELTA, "{\"text\":\"" + escape(t.text()) + "\"}");
                    }
                });
    }

    /**
     * 同轮工具调用：事件顺序与执行并发分离。
     * 0) 审批预判先于执行：命中高风险工具则整批挂起，避免"先执行一半再挂起"的半成品状态；
     * 1) 重复调用检测与计数顺序执行，保证并发下不漏检；
     * 2) TOOL_CALL 按模型原始顺序全部先发；
     * 3) 工具本身有界并行，TOOL_RESULT 按完成顺序发（前端靠 callId 关联）；
     * 4) 结果回灌上下文时按原始下标排序，模型看到的顺序稳定可复现；
     * 5) 单个失败降级为失败结果（带 error 文本）回灌，全部失败才向上抛错；
     * 6) 挂起不是失败：返回 suspended 信号，由 loop() 以正常完成结束本轮。
     */
    private Mono<StepOutcome> executeTools(RunContext ctx, List<ToolCall> calls, GenAiSpan parent) {
        if (calls.isEmpty()) {
            return Mono.just(StepOutcome.next());
        }

        // ⓪ 审批预判：待审批批次整体挂起，本轮不执行任何工具
        for (ToolCall call : calls) {
            if (!ctx.isCallApproved(call.id()) && toolExecutor.requiresApproval(call)) {
                ctx.suspend(calls);
                ctx.markSuspended();
                return toolExecutor.awaitApproval(ctx, call)
                        .thenReturn(StepOutcome.suspendedOutcome());
            }
        }

        for (ToolCall call : calls) {
            var repeat = loopGuard.checkRepeat(ctx, call.name(), call.argumentsJson());
            if (repeat.isPresent()) {
                return Mono.error(new AgentException(repeat.get().code(), repeat.get().message()));
            }
            ctx.incToolCall();
        }

        for (ToolCall call : calls) {
            ctx.emit(EventType.TOOL_CALL, "{\"callId\":\"" + call.id()
                    + "\",\"name\":\"" + call.name() + "\"}");
        }

        List<Mono<IndexedResult>> monos = new ArrayList<>(calls.size());
        for (int i = 0; i < calls.size(); i++) {
            monos.add(executeOne(ctx, calls.get(i), i, parent));
        }
        AgentFlowProperties.Parallel parallel = props.getTool().getParallel();
        Flux<IndexedResult> executed = parallel.isEnabled()
                ? Flux.fromIterable(monos).flatMap(mono -> mono, Math.max(1, parallel.getMaxConcurrency()))
                // 关闭并行开关后退回原串行语义
                : Flux.concat(monos);

        return executed.collectList().flatMap(results -> {
            // 防御：工具执行阶段才暴露的审批需求同样按挂起处理，绝不判 TOOL_ALL_FAILED
            if (results.stream().anyMatch(r -> r.result().approvalRequired())) {
                ctx.suspend(calls);
                ctx.markSuspended();
                return Mono.just(StepOutcome.suspendedOutcome());
            }
            if (results.stream().noneMatch(r -> r.result().success())) {
                return Mono.error(new AgentException("TOOL_ALL_FAILED",
                        "本轮 " + results.size() + " 个工具调用全部失败"));
            }
            List<IndexedResult> ordered = new ArrayList<>(results);
            ordered.sort(Comparator.comparingInt(IndexedResult::index));
            int maxChars = Math.max(256, props.getAgent().getToolResultMaxTokens() * 3);
            for (IndexedResult result : ordered) {
                ctx.append(ChatMessage.tool(result.call().id(), result.result().toModelContent(maxChars)));
            }
            return Mono.just(StepOutcome.next());
        });
    }

    private Mono<IndexedResult> executeOne(RunContext ctx, ToolCall call, int index, GenAiSpan parent) {
        return Mono.defer(() -> {
            // 工具 span 的父上下文由参数显式传入，并行/切换线程都不依赖 ThreadLocal
            GenAiSpan toolSpan = telemetry.beginTool(parent, call.name());
            return toolExecutor.execute(call, ctx)
                    // 单点失败转失败结果，由模型自行纠错；不拖垮整轮
                    .onErrorResume(e -> Mono.just(toFailure(e)))
                    .doOnNext(r -> {
                        toolSpan.recordToolResult(r.success());
                        ctx.emit(EventType.TOOL_RESULT,
                                "{\"callId\":\"" + call.id() + "\",\"ok\":" + r.success()
                                        + ",\"preview\":\"" + escape(r.preview()) + "\"}");
                    })
                    .doOnError(toolSpan::recordError)
                    .doFinally(signal -> toolSpan.end())
                    .map(r -> new IndexedResult(index, call, r));
        });
    }

    private ToolResult toFailure(Throwable e) {
        String code = e instanceof AgentException ae ? ae.getCode() : "TOOL_ERROR";
        String message = e.getMessage() == null ? e.toString() : e.getMessage();
        return ToolResult.fail(code, message);
    }

    /** 工具执行结果与原始下标绑定，用于乱序完成后按序回灌 */
    private record IndexedResult(int index, ToolCall call, ToolResult result) {
    }

    /**
     * 一步的结果：finished=true 拿到最终答复；suspended=true 等待人工审批（正常暂停，不是失败）；
     * 两者都为 false 表示还有工具结果待进入下一步。
     */
    private record StepOutcome(boolean finished, boolean suspended, String text) {
        static StepOutcome done(String text) {
            return new StepOutcome(true, false, text);
        }

        static StepOutcome next() {
            return new StepOutcome(false, false, null);
        }

        /** 注意：不能命名为 suspended()，会与 record 组件 suspended 的访问器签名冲突 */
        static StepOutcome suspendedOutcome() {
            return new StepOutcome(false, true, null);
        }
    }

    private String escape(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "");
    }
}
