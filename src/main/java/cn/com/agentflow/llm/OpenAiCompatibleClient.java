package cn.com.agentflow.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.List;

@Slf4j
public class OpenAiCompatibleClient implements ModelPort {

    private final String modelName;
    private final int contextWindow;
    private final WebClient webClient;
    private final ObjectMapper mapper;

    public OpenAiCompatibleClient(String baseUrl, String apiKey, String modelName,
                                  int contextWindow, ObjectMapper mapper, int timeoutMs) {
        this.modelName = modelName;
        this.contextWindow = contextWindow;
        this.mapper = mapper;
        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader("Authorization", "Bearer " + apiKey)
                .codecs(c -> c.defaultCodecs().maxInMemorySize(4 * 1024 * 1024))
                .build();
    }

    @Override
    public String modelName() {
        return modelName;
    }

    @Override
    public int contextWindow() {
        return contextWindow;
    }

    @Override
    public Flux<ChatChunk> stream(ChatRequest request) {
        // 冷流：每次订阅才发请求；取消会传播到 WebClient 关闭连接
        return Flux.defer(() -> webClient.post()
                        .uri("/v1/chat/completions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.TEXT_EVENT_STREAM)
                        .bodyValue(buildPayload(request))
                        .retrieve()
                        .bodyToFlux(String.class))
                .takeWhile(line -> !"[DONE]".equals(line.trim()))
                .concatMap(this::parseSafely)
                .doOnCancel(() -> log.info("模型流被取消, model={}", modelName));
    }

    private ObjectNode buildPayload(ChatRequest request) {
        ObjectNode root = mapper.createObjectNode();
        root.put("model", request.model());
        root.put("stream", true);
        root.put("temperature", request.temperature());
        root.put("max_tokens", request.maxTokens());
        root.putObject("stream_options").put("include_usage", true);

        ArrayNode msgs = root.putArray("messages");
        for (ChatMessage m : request.messages()) {
            ObjectNode n = msgs.addObject();
            n.put("role", m.role());
            if (m.content() != null) {
                n.put("content", m.content());
            }
            if (m.toolCallId() != null) {
                n.put("tool_call_id", m.toolCallId());
            }
            if (m.toolCalls() != null && !m.toolCalls().isEmpty()) {
                ArrayNode arr = n.putArray("tool_calls");
                for (ToolCall tc : m.toolCalls()) {
                    ObjectNode o = arr.addObject();
                    o.put("id", tc.id());
                    o.put("type", "function");
                    ObjectNode fn = o.putObject("function");
                    fn.put("name", tc.name());
                    fn.put("arguments", tc.argumentsJson());
                }
            }
        }

        List<ToolDef> tools = request.tools();
        if (tools != null && !tools.isEmpty()) {
            ArrayNode arr = root.putArray("tools");
            for (ToolDef t : tools) {
                ObjectNode o = arr.addObject();
                o.put("type", "function");
                ObjectNode fn = o.putObject("function");
                fn.put("name", t.name());
                fn.put("description", t.description());
                fn.set("parameters", t.parameters());
            }
        }
        return root;
    }

    /** 单行解析失败只记日志，不影响整条流（供应商偶尔会带心跳行） */
    private Flux<ChatChunk> parseSafely(String line) {
        try {
            return parse(line);
        } catch (Exception e) {
            log.debug("忽略无法解析的 SSE 行: {}", line);
            return Flux.empty();
        }
    }

    private Flux<ChatChunk> parse(String line) throws Exception {
        JsonNode root = mapper.readTree(line);

        JsonNode usage = root.path("usage");
        if (usage.isObject() && usage.has("prompt_tokens")) {
            return Flux.just(new ChatChunk.Usage(
                    usage.path("prompt_tokens").asInt(),
                    usage.path("completion_tokens").asInt()));
        }

        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            return Flux.empty();
        }
        JsonNode choice = choices.get(0);
        JsonNode delta = choice.path("delta");

        Flux<ChatChunk> text = Flux.empty();
        if (delta.hasNonNull("content") && !delta.get("content").asText().isEmpty()) {
            text = Flux.just(new ChatChunk.TextDelta(delta.get("content").asText()));
        }

        Flux<ChatChunk> tools = Flux.empty();
        JsonNode toolCalls = delta.path("tool_calls");
        if (toolCalls.isArray() && !toolCalls.isEmpty()) {
            tools = Flux.fromIterable(toolCalls).map(tc -> new ChatChunk.ToolCallDelta(
                    tc.path("index").asInt(0),
                    tc.path("id").asText(null),
                    tc.path("function").path("name").asText(null),
                    tc.path("function").path("arguments").asText("")));
        }

        String finish = choice.path("finish_reason").asText(null);
        Flux<ChatChunk> fin = finish == null ? Flux.empty()
                : Flux.just(new ChatChunk.Finished(finish));

        return Flux.concat(text, tools, fin);
    }
}
