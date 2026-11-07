package cn.com.agentflow.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * agentflow.* 配置总入口，字段与 application.yml 完全对齐。
 * 直接注册为 Bean（不依赖主类上的 @EnableConfigurationProperties 也能生效）。
 */
@Slf4j
@Data
@Component
@ConfigurationProperties(prefix = "agentflow")
public class AgentFlowProperties {

    /** max-duration-ms 的兜底默认值 */
    private static final long DEFAULT_MAX_DURATION_MS = 300_000;

    /** 实例标识：多实例部署时用于租约归属 */
    private String instanceId = "local";

    private Llm llm = new Llm();

    private Agent agent = new Agent();

    private Stream stream = new Stream();

    private Run run = new Run();

    private Quota quota = new Quota();

    private Memory memory = new Memory();

    private Idempotency idempotency = new Idempotency();

    private Tool tool = new Tool();

    private Approval approval = new Approval();

    private Telemetry telemetry = new Telemetry();

    private Prompt prompt = new Prompt();

    /**
     * GenAI 可观测性配置：内容默认不入 span（PII 与体积双重考虑），
     * 采样率驱动全局 OTel Sampler，避免与 management.tracing 双重采样。
     */
    @Data
    public static class Telemetry {
        /** 是否把 prompt/completion 内容写入 span；默认 false，只记长度与 token 数 */
        private boolean captureContent = false;
        /** run 级根采样率 0~1，默认 0.1；子 span 通过 parentBased 继承 */
        private double sampleRate = 0.1;
        /** 内容写入 span 时的单条上限（字符），脱敏后再截断 */
        private int maxContentChars = 4096;

        /** 非法值收敛：NaN/负数按 0，大于 1 按 1，避免 Sampler 抛错 */
        public double effectiveSampleRate() {
            if (Double.isNaN(sampleRate) || sampleRate <= 0) {
                return 0.0;
            }
            return Math.min(1.0, sampleRate);
        }
    }

    @Data
    public static class Prompt {
        /** 是否在 DEBUG 日志打印前缀指纹，便于验证缓存可复用性 */
        private boolean logFingerprint = true;
    }

    @Data
    public static class Llm {
        /** 默认模型名，必须存在于 models 中 */
        private String defaultModel = "mock-large";
        /** 模型名 → 接入配置 */
        private Map<String, Model> models = new LinkedHashMap<>();
        /** 金丝雀灰度路由：默认关闭，开启后按权重把 from 的流量切到 to */
        private Canary canary = new Canary();
        /** 单次请求最多尝试的模型个数（含首选），防止兜底链过长放大故障 */
        private int maxAttempts = 3;
        /**
         * 全局默认 fallback 链（有序）：模型未单独配置 fallback-chain 时生效。
         * 首选失败（5xx/超时/上下文超长）后按链依次切换，链内自动去重并跳过未注册模型。
         */
        private List<String> fallbackChain = new ArrayList<>();
        /** 单次请求成本预算（美元）；<=0 表示不做成本约束 */
        private double maxCostPerRequest = 0.0;
        /** 失败模型冷却（熔断）：连续失败达阈值打开，到期半开少量探测 */
        private Cooldown cooldown = new Cooldown();
    }

    /** 熔断冷却参数：全模型共享一套阈值，按上游整体稳定性调参 */
    @Data
    public static class Cooldown {
        /** 连续失败达到该次数即打开熔断 */
        private int failureThreshold = 3;
        /** 冷却时长：期间该模型直接跳过 */
        private long openMs = 30000;
        /** 半开期只放行的探测请求数，探测成功闭合、失败重新打开 */
        private int halfOpenProbes = 1;
    }

    @Data
    public static class Canary {
        private boolean enabled = false;
        /** 源模型：只有首选路由解析到它时才参与灰度 */
        private String from;
        /** 目标（新版本）模型 */
        private String to;
        /** 切到 to 的权重百分比 0~100 */
        private int weightPercent = 0;
        /** 租户粘性：同租户哈希分桶，保证多轮对话模型一致；关闭则每请求随机 */
        private boolean stickyByTenant = true;
    }

    @Data
    public static class Model {
        private String baseUrl;
        private String model;
        private String apiKey;
        private int contextWindow = 8192;
        private int rpmLimit = 600;
        private int tpmLimit = 200000;
        private int maxConcurrency = 64;
        private long timeoutMs = 60000;
        /** 首包超时：TTFT 差说明上游异常，比整体超时更早暴露 */
        private long ttftTimeoutMs = 15000;
        /** AIMD 自适应并发舱壁：默认关闭保持写死并发；开启后按成功窗口 +1、失败 ×0.7 动态调整 */
        private boolean adaptiveConcurrency = false;
        /** AIMD 退化信号阈值：TTFT 超过该毫秒按失败处理（乘性减） */
        private long adaptiveTtftThresholdMs = 3000;
        /** 普通兜底链（有序）：主模型可重试失败后依次尝试 */
        private List<String> fallbacks = new ArrayList<>();
        /**
         * 模型级 fallback 链（有序），优先级高于全局 fallback-chain 与旧字段 fallbacks。
         * 例如 {@code fallback-chain: [mock-small]}：mock-large 失败后切到 mock-small。
         */
        private List<String> fallbackChain = new ArrayList<>();
        /** 该模型的冷却时长（毫秒）；为空时使用 agentflow.llm.cooldown.open-ms */
        private Long cooldownMs;
        /** 上下文超限兜底链：优先换到上下文窗口更大的模型，而不是普通兜底 */
        private List<String> contextWindowFallbacks = new ArrayList<>();
        /** 相对成本：每 1K token 单价（美元），0 表示未配置、不参与成本约束 */
        private double costPer1kTokens = 0.0;
    }

    @Data
    public static class Agent {
        private int maxSteps = 12;
        private int maxToolCalls = 8;
        private long maxWallClockMs = 120000;
        /** 单个 run 的墙钟总预算（硬截止）：到点按 RUN_TIMEOUT 失败收尾 */
        private long maxDurationMs = DEFAULT_MAX_DURATION_MS;
        /** 单步预算：一次 LLM 调用 + 该步工具执行；超时按可重试失败处理一次 */
        private long stepTimeoutMs = 60000;
        private int defaultTokenBudget = 32000;
        /** 历史占用超过预算该比例时触发摘要压缩 */
        private double compressorTriggerRatio = 0.90;
        private int toolResultMaxTokens = 2000;
        private int summaryMaxTokens = 500;
        private Ratio tokenBudgetRatio = new Ratio();
        /** 长期记忆预检索：开跑前召回一次并注入 system prompt */
        private MemoryPreRetrieval memoryPreRetrieval = new MemoryPreRetrieval();
    }

    /**
     * 启动期收敛超时配置，不抛异常（配置错误不应导致进程起不来）：
     * 单步预算必须严格小于 run 总预算，否则按总预算收敛并 WARN。
     */
    @PostConstruct
    void normalizeTimeoutBudget() {
        if (agent.getMaxDurationMs() <= 0) {
            log.warn("agentflow.agent.max-duration-ms={} 非法，回退默认 {}ms",
                    agent.getMaxDurationMs(), DEFAULT_MAX_DURATION_MS);
            agent.setMaxDurationMs(DEFAULT_MAX_DURATION_MS);
        }
        long maxDuration = agent.getMaxDurationMs();
        long stepTimeout = agent.getStepTimeoutMs();
        if (stepTimeout <= 0 || stepTimeout >= maxDuration) {
            log.warn("agentflow.agent.step-timeout-ms={} 必须大于 0 且小于 max-duration-ms={}，"
                    + "按 max-duration 收敛为 {}ms", stepTimeout, maxDuration, maxDuration);
            agent.setStepTimeoutMs(maxDuration);
        }
    }

    /** 预检索注入配置：召回失败只降级为“无记忆”，不影响 run */
    @Data
    public static class MemoryPreRetrieval {
        private boolean enabled = true;
        private int topK = 5;
    }

    @Data
    public static class Tool {
        /** 同轮工具调用并行执行配置 */
        private Parallel parallel = new Parallel();
    }

    /** 有界并行：maxConcurrency 同时限制在途工具数，关闭后退回串行 */
    @Data
    public static class Parallel {
        private boolean enabled = true;
        private int maxConcurrency = 4;
    }

    /**
     * 人工审批（HITL）：超时未裁决由对账任务/审批入口惰性收敛为 TIMEOUT 并释放资源。
     * 不新建调度器，复用 RunRecoveryJob 的扫描周期。
     */
    @Data
    public static class Approval {
        /** 待审批单最长存活时间，默认 30 分钟 */
        private Duration timeout = Duration.ofMinutes(30);
    }

    /** token 预算分配比例：system/tools/history/output 之和应为 1 */
    @Data
    public static class Ratio {
        private double system = 0.10;
        private double tools = 0.15;
        private double history = 0.45;
        private double output = 0.30;
    }

    @Data
    public static class Stream {
        private int heartbeatSeconds = 15;
        private int replayBufferSize = 500;
        private int replayTtlSeconds = 1800;
        private int maxConnectionsPerTenant = 200;
        /** 单连接有界缓冲条数：满了丢最旧，保证新事件时效，正常消费者无感知 */
        private int writeBufferSize = 256;
        /** 单连接连续丢弃超过该阈值即主动断开，客户端带 Last-Event-ID 重连回放 */
        private int maxLagEvents = 200;
        private int writeTimeoutSeconds = 5;
    }

    @Data
    public static class Run {
        /** DB 租约秒数：实例崩溃后租约过期，其他实例才能接管 */
        private int leaseSeconds = 60;
        private long recoveryIntervalMs = 5000;
        private int recoveryBatch = 50;
    }

    @Data
    public static class Quota {
        /** 本地预检开关：Redis 挂之前的快速兜底 */
        private boolean localPrecheckEnabled = true;
        /** 本地桶容量 = 全局 / 实例数 * localFactor */
        private double localFactor = 1.2;
        private int dailyRunLimitFree = 200;
        private int dailyRunLimitStandard = 5000;
        private int dailyRunLimitPremium = 100000;
        /** 预扣 Saga 对账补偿任务 */
        private Reconcile reconcile = new Reconcile();

        @Data
        public static class Reconcile {
            /** 对账开关：关闭后不再自动退款，仅保留账本 */
            private boolean enabled = true;
            /** 扫描间隔（毫秒），与 @Scheduled fixedDelay 对齐 */
            private long intervalMs = 60000;
            /** 账本超过该时长仍非终态即视为陈旧，进入补偿 */
            private Duration staleAfter = Duration.ofMinutes(5);
            /** 单轮扫描批量，限制一次补偿占用的事务/连接 */
            private int batchSize = 200;
        }
    }

    @Data
    public static class Memory {
        private int recallTopK = 5;
        /** 余弦相似度阈值，低于该值不召回 */
        private double recallThreshold = 0.75;
        /** 时效衰减系数：score = similarity * weight * exp(-lambda * 天数) */
        private double weightLambda = 0.01;
        /** 向量维度，与 DDL 中 vector(1024) 对齐 */
        private int embeddingDim = 1024;
        /** embedding 服务接入（OpenAI 兼容 /v1/embeddings）；缺省指向 mock-llm */
        private String embeddingBaseUrl = "http://localhost:9000";
        private String embeddingModel = "mock-embedding";
        private String embeddingApiKey = "mock";
        private long embeddingTimeoutMs = 10000;
    }

    @Data
    public static class Idempotency {
        /** 幂等层总开关：关闭后所有请求按原逻辑放行 */
        private boolean enabled = true;
        /** 是否强制携带 Idempotency-Key：默认关闭，保持对老客户端的向后兼容 */
        private boolean requireKey = false;
        /** 幂等状态 TTL：覆盖"处理中"锁与已完成响应的保留窗口 */
        private Duration ttl = Duration.ofMinutes(10);
        /** Idempotency-Key 最大长度，超长直接 400，不打 Redis */
        private int keyMaxLength = 128;
    }
}
