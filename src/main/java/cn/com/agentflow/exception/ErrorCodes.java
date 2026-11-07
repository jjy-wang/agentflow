package cn.com.agentflow.exception;

/**
 * 全局业务错误码。集中定义避免同一语义在不同层散落成不同字面量，
 * 也避免把“本地限流拒绝”误报成“没有可用模型”。
 *
 * <p>语义边界：</p>
 * <ul>
 *   <li>{@link #RATE_LIMITED}：本地预检令牌桶拒绝（单机快速失败，不代表全局额度耗尽）；</li>
 *   <li>{@link #QUOTA_EXCEEDED}：Redis 分布式令牌桶拒绝（全局额度确实不足）；</li>
 *   <li>{@link #MODEL_BUSY}：模型舱壁已满或候选模型均处于冷却期（容量/临时不可用）；</li>
 *   <li>{@link #NO_MODEL_AVAILABLE}：路由确实找不到任何已注册模型；</li>
 *   <li>{@link #UPSTREAM_ERROR}：候选模型都真正调用过但全部失败（5xx/超时等）。</li>
 * </ul>
 */
public final class ErrorCodes {

    public static final String RATE_LIMITED = "RATE_LIMITED";
    public static final String QUOTA_EXCEEDED = "QUOTA_EXCEEDED";
    public static final String MODEL_BUSY = "MODEL_BUSY";
    public static final String NO_MODEL_AVAILABLE = "NO_MODEL_AVAILABLE";
    public static final String UPSTREAM_ERROR = "UPSTREAM_ERROR";

    private ErrorCodes() {
    }
}
