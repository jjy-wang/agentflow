package cn.com.agentflow.tool;

import cn.com.agentflow.agent.RunContext;
import cn.com.agentflow.exception.AgentException;
import cn.com.agentflow.llm.ToolCall;
import cn.com.agentflow.run.ApprovalService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 工具执行器：schema 校验 + 高风险审批 + 幂等 + 超时控制
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class ToolExecutor {

    private final ToolRegistry registry;
    private final ToolInvocationStore invocationStore;
    private final ApprovalService approvalService;
    private final ObjectMapper mapper;

    private final Map<String, JsonSchema> schemaCache = new ConcurrentHashMap<>();

    /**
     * 是否属于需要人工审批的高风险工具。
     * 供 ReActLoop 在执行前整轮预判，避免"先执行一半再挂起"的半成品状态。
     */
    public boolean requiresApproval(ToolCall call) {
        return registry.find(call.name())
                .map(meta -> meta.riskLevel() >= 3)
                .orElse(false);
    }

    /**
     * 挂起并等待审批：写审批单 + 持久化待执行工具快照 + 释放 run 租约。
     * 由 ReActLoop 在整轮预判命中后调用，调用方随后以挂起信号（而非失败）结束本步。
     */
    public Mono<Void> awaitApproval(RunContext ctx, ToolCall pending) {
        return Mono.defer(() -> registry.find(pending.name())
                .map(meta -> approvalService.requestApproval(ctx, pending, meta)
                        .flatMap(approvalId -> approvalService.suspend(ctx, approvalId)))
                .orElseGet(() -> Mono.error(new AgentException("TOOL_NOT_FOUND",
                        "工具不存在: " + pending.name()))));
    }

    public Mono<ToolResult> execute(ToolCall call, RunContext ctx) {
        return Mono.defer(() -> {
            ToolMeta meta = registry.find(call.name())
                    .orElseThrow(() -> new AgentException("TOOL_NOT_FOUND", "工具不存在: " + call.name()));

            // ① schema 校验（模型幻觉参数在这里被拦）
            var errors = schema(meta).validate(readJson(call.argumentsJson()));
            if (!errors.isEmpty()) {
                String msg = errors.stream().findFirst().map(e -> e.getMessage()).orElse("参数非法");
                return Mono.just(ToolResult.fail("TOOL_ARGS_INVALID", msg));
            }

            // ② 高风险工具 → 挂起（不是失败）；ReActLoop 已整批预判，这里兜底竞态
            if (meta.riskLevel() >= 3 && !ctx.isCallApproved(call.id())) {
                ctx.suspend(java.util.List.of(call));
                ctx.markSuspended();
                return awaitApproval(ctx, call).thenReturn(ToolResult.approvalRequired(call.name()));
            }

            // ③ 幂等：以 (runId, toolCallId) 为准，插入成功才执行
            return invocationStore.tryStart(ctx.getRunId(), call.id(), call.name())
                    .flatMap(started -> {
                        if (!started) {
                            return invocationStore.findResult(ctx.getRunId(), call.id())
                                    .map(this::toResult);
                        }
                        long start = System.currentTimeMillis();
                        return Mono.fromCallable(() -> meta.handler().execute(readJson(call.argumentsJson()), ctx))
                                .subscribeOn(Schedulers.boundedElastic())
                                .timeout(Duration.ofMillis(meta.timeoutMs()))
                                .onErrorResume(e -> Mono.just(ToolResult.fail("TOOL_ERROR", e.getMessage())))
                                .flatMap(r -> invocationStore.finish(ctx.getRunId(), call.id(), r)
                                        .thenReturn(r))
                                .doOnNext(r -> log.info("工具执行完成 run={} tool={} cost={}ms",
                                        ctx.getRunId(), call.name(), System.currentTimeMillis() - start));
                    });
        });
    }

    private JsonSchema schema(ToolMeta meta) {
        return schemaCache.computeIfAbsent(meta.name(), k -> JsonSchemaFactory
                .getInstance(SpecVersion.VersionFlag.V202012)
                .getSchema(meta.schema()));
    }

    private com.fasterxml.jackson.databind.JsonNode readJson(String json) {
        try {
            return mapper.readTree(json == null || json.isBlank() ? "{}" : json);
        } catch (Exception e) {
            throw new AgentException("TOOL_ARGS_INVALID", "参数不是合法 JSON");
        }
    }

    private ToolResult toResult(String storedJson) {
        try {
            return mapper.readValue(storedJson, ToolResult.class);
        } catch (Exception e) {
            return ToolResult.fail("TOOL_CACHE_ERROR", e.getMessage());
        }
    }
}
