package cn.com.agentflow.llm;

import reactor.core.publisher.Flux;

/**
 * 模型端口。实现必须满足三条：
 * 1) stream() 是冷流，订阅时才发起请求；
 * 2) 下游取消时关闭上游连接（doOnCancel 传播）；
 * 3) 内部禁止阻塞调用。
 */
public interface ModelPort {

    String modelName();

    int contextWindow();

    Flux<ChatChunk> stream(ChatRequest request);
}
