package cn.com.agentflow.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 假模型服务：压测与集成测试专用。
 * 通过请求头控制行为，避免改代码：
 *   X-Mock-Delay-Ms  : 首包前延迟（模拟 TTFT）
 *   X-Mock-Tps       : 每秒产出 token 数
 *   X-Mock-Tokens    : 本次输出 token 数
 *   X-Mock-Fail-Rate : 失败概率 0~1
 *   X-Mock-Fail-Mode : error / timeout / half
 *   X-Mock-Tool      : 非空时返回一次工具调用（值为工具名）
 */
@SpringBootApplication
@RestController
public class MockLlmApplication {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicInteger SEQ = new AtomicInteger();

    public static void main(String[] args) {
        SpringApplication.run(MockLlmApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "UP", "served", SEQ.get());
    }

    @PostMapping(value = "/v1/chat/completions", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<Flux<String>> completions(
            @RequestBody JsonNode body,
            @RequestHeader(value = "X-Mock-Delay-Ms", defaultValue = "200") long delayMs,
            @RequestHeader(value = "X-Mock-Tps", defaultValue = "50") int tps,
            @RequestHeader(value = "X-Mock-Tokens", defaultValue = "128") int tokens,
            @RequestHeader(value = "X-Mock-Fail-Rate", defaultValue = "0") double failRate,
            @RequestHeader(value = "X-Mock-Fail-Mode", defaultValue = "error") String failMode,
            @RequestHeader(value = "X-Mock-Tool", defaultValue = "") String mockTool) {

        SEQ.incrementAndGet();
        int promptTokens = body.path("messages").toString().length() / 2;
        long intervalMs = Math.max(1, 1000L / Math.max(1, tps));

        // 失败模式一：直接 500
        if ("error".equals(failMode) && ThreadLocalRandom.current().nextDouble() < failRate) {
            return ResponseEntity.status(500).body(Flux.just(done()));
        }
        // 失败模式二：迟迟不返回首包（验证 TTFT 超时）
        long actualDelay = "timeout".equals(failMode)
                && ThreadLocalRandom.current().nextDouble() < failRate ? 120_000 : delayMs;

        boolean toolMode = !mockTool.isBlank();
        if (!toolMode) {
            // 未显式指定 X-Mock-Tool 时按用户问题自动选择工具，便于真实链路联调；
            // 已有 tool 角色消息（即工具结果已回传）时不再重复调用，避免死循环。
            mockTool = autoTool(body);
            toolMode = !mockTool.isBlank();
        }
        String[] args = toolArgs(mockTool);

        Flux<String> stream = Flux.concat(
                        // 首包延迟
                        Flux.<String>empty().delaySubscription(Duration.ofMillis(actualDelay)),
                        // 工具调用分片（故意拆成 3 段，验证客户端拼装）
                        toolMode ? Flux.just(toolChunk(0, "call_" + SEQ.get(), mockTool, ""),
                                toolChunk(0, null, null, args[0]),
                                toolChunk(0, null, null, args[1])) : Flux.empty(),
                        // 文本增量
                        toolMode ? Flux.empty()
                                 : Flux.range(0, Math.max(1, tokens))
                                       .delayElements(Duration.ofMillis(intervalMs))
                                       .map(i -> textChunk("token" + i + " ")),
                        // 用完量
                        Flux.just(usageChunk(promptTokens, tokens)),
                        // 结束
                        Flux.just(finishedChunk(toolMode ? "tool_calls" : "stop")))
                // 失败模式三：吐一半断开
                .takeUntilOther(("half".equals(failMode)
                        && ThreadLocalRandom.current().nextDouble() < failRate)
                        ? Flux.just("x").delaySubscription(Duration.ofMillis(intervalMs * Math.max(1, tokens / 2)))
                        : Flux.never());

        return ResponseEntity.ok().contentType(MediaType.TEXT_EVENT_STREAM).body(stream);
    }

    private String done() {
        return "[DONE]";
    }

    /** 按最后一条用户消息猜测要调用的工具；工具结果回传后返回空，转为文本收尾 */
    private String autoTool(JsonNode body) {
        JsonNode tools = body.path("tools");
        if (!tools.isArray() || tools.size() == 0) {
            return "";
        }
        String question = "";
        for (JsonNode m : body.path("messages")) {
            String role = m.path("role").asText();
            if ("tool".equals(role) || "function".equals(role)) {
                return "";
            }
            if ("user".equals(role)) {
                question = m.path("content").asText("");
            }
        }
        if (question.contains("删除") || question.contains("下线") || question.contains("移除")) {
            return "asset_delete";
        }
        if (question.contains("资产") || question.contains("主机")
                || question.contains("暴露面") || question.contains("检索")) {
            return "asset_search";
        }
        return "";
    }

    /** 工具参数分两段输出，客户端需要跨 chunk 拼接 */
    private String[] toolArgs(String tool) {
        if ("asset_delete".equals(tool) || "asset_detail".equals(tool)) {
            return new String[]{"{\"assetId\":", "1}"};
        }
        return new String[]{"{\"keyword\":", "\"10.0\"}"};
    }

    private String textChunk(String text) {
        return wrap("{\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + text.replace("\"", "'") + "\"}}]}");
    }

    private String toolChunk(int index, String id, String name, String args) {
        StringBuilder sb = new StringBuilder("{\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":")
                .append(index).append(",\"function\":{");
        if (id != null) {
            sb.append("\"id\":\"").append(id).append("\",");
        }
        if (name != null) {
            sb.append("\"name\":\"").append(name).append("\",");
        }
        sb.append("\"arguments\":\"").append(args.replace("\"", "\\\"")).append("\"}}]}}]}");
        return wrap(sb.toString());
    }

    private String usageChunk(int in, int out) {
        return wrap("{\"usage\":{\"prompt_tokens\":" + in + ",\"completion_tokens\":" + out + "}}");
    }

    private String finishedChunk(String reason) {
        return wrap("{\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"" + reason + "\"}]}");
    }

    private String wrap(String json) {
        return json;
    }
}
