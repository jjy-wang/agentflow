# ADR 0002：自研 LLM 网关而非用 SDK

- 状态：已采纳
- 日期：2026-10

## 背景

平台要同时对接多家 OpenAI 兼容模型供应商，并且要在调用链上做精细控制：逐 token 流式、TTFT 单独计时、RPM/TPM 预扣结算、每模型舱壁信号量、取消时释放 HTTP 连接。现成 SDK（OpenAI 官方 Java SDK、Spring AI、LangChain4j）把这些细节封装在高级抽象后面：流式背压语义不透明、取消不保证释放底层连接、限流钩子位置不对（SDK 内部重试会绕过外层计数）。

## 决策

自研薄网关层（`llm/` 包）：`ModelPort` 定义流式接口，`OpenAiCompatibleClient` 基于 WebClient 实现 OpenAI 兼容 SSE 协议解析，`ModelRegistry` 按配置装配模型（`base-url`、上下文窗口、RPM/TPM、并发、超时），`ModelRouter` 按请求与策略选模型，`LlmInvoker` 把配额预扣、舱壁、计时、取消编排成一条 Reactor 链。

## 备选方案

1. **Spring AI / LangChain4j**：被否。要对流式、背压、限流、取消精细控制，框架抽象会把问题藏起来——例如取消订阅时 SDK 是否关闭上游连接不可控，多供应商 RPM/TPM 配额也没有对应扩展点。
2. **OpenAI 官方 Java SDK**：被否。强绑定 OpenAI 协议细节，对兼容供应商的流式差异（空行、`data: [DONE]`、keep-alive 注释行）处理不可定制。
3. **直连供应商不做网关**：被否。限流、舱壁、路由将散落到各调用方，无法统一治理与观测。

## 代价

- **多写约 1500 行**：协议解析、重连、错误映射、流式分帧都要自己实现与测试；
- **要跟协议变更**：供应商流式协议演进（新事件类型、usage 字段位置变化）需要自行跟进，SDK 用户则等官方升级；
- **测试负担**：必须维护 mock-llm 子工程模拟供应商行为（延迟、TPS、工具调用、错误注入），否则集成测试无从谈起。

## 当前实现要点

- 协议解析按行分帧：空行分隔事件、`data: [DONE]` 终止、忽略 keep-alive 注释行；
- TTFT 从请求发出到首个内容 chunk 单独计时（`agent.ttft` 指标），与端到端时延分开观测；
- 取消订阅即关闭上游 HTTP 连接，配合舱壁信号量与配额预扣在 `doFinally` 中统一释放；
- 错误映射：上游 4xx/5xx/超时统一映射为 `llm.error.total{type=...}` 与可重试/不可重试语义。

## 验证

- `ModelRouterTest` 覆盖路由与摘除逻辑；集成测试经 mock-llm 注入延迟、错误与工具调用，验证解析与取消路径；
- 压测侧 `agent.ttft`、`llm.error.total` 指标在 6 个 k6 场景中持续产出，证明埋点与流式链路工作正常（见 `docs/BENCHMARK.md`）。

## 结论

用约 1500 行自研代码换取对流式链路背压、取消、限流的完全可控，是本项目的核心差异化能力，值得承担跟进协议变更的成本。网关接口（`ModelPort`）保持稳定，未来若要引入 SDK，可在 `ModelPort` 之下替换实现而不影响上层。
