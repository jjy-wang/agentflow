package cn.com.agentflow.llm;

import cn.com.agentflow.config.AgentFlowProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Component
public class ModelRegistry {

    private final Map<String, ModelPort> ports = new LinkedHashMap<>();
    private final Map<String, AgentFlowProperties.Model> configs = new LinkedHashMap<>();

    public ModelRegistry(AgentFlowProperties props, ObjectMapper mapper) {
        props.getLlm().getModels().forEach((name, cfg) -> {
            configs.put(name, cfg);
            ports.put(name, new OpenAiCompatibleClient(
                    cfg.getBaseUrl(), cfg.getApiKey(), cfg.getModel(),
                    cfg.getContextWindow(), mapper, Math.toIntExact(cfg.getTimeoutMs())));
            log.info("注册模型: name={}, model={}, rpm={}, tpm={}, concurrency={}, fallbackChain={}, cooldownMs={}, contextFallbacks={}",
                    name, cfg.getModel(), cfg.getRpmLimit(), cfg.getTpmLimit(), cfg.getMaxConcurrency(),
                    cfg.getFallbackChain().isEmpty() ? cfg.getFallbacks() : cfg.getFallbackChain(),
                    cfg.getCooldownMs(), cfg.getContextWindowFallbacks());
        });
    }

    public ModelPort port(String name) {
        ModelPort p = ports.get(name);
        if (p == null) {
            throw new IllegalArgumentException("未注册的模型: " + name);
        }
        return p;
    }

    public AgentFlowProperties.Model config(String name) {
        return configs.get(name);
    }

    public boolean contains(String name) {
        return ports.containsKey(name);
    }

    public Map<String, ModelPort> all() {
        return ports;
    }
}
