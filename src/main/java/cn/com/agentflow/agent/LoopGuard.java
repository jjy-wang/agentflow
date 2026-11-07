package cn.com.agentflow.agent;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;

/**
 * 防死循环：
 * 1) 最大步数  2) 最大工具调用次数  3) 墙钟超时  4) 重复调用指纹  5) token 预算
 */
@Component
public class LoopGuard {

    public record Violation(String code, String message) {
    }

    public Optional<Violation> check(RunContext ctx) {
        if (ctx.getStep() >= ctx.getLimits().maxSteps()) {
            return Optional.of(new Violation("STEP_LIMIT",
                    "超过最大步数 " + ctx.getLimits().maxSteps()));
        }
        if (ctx.getToolCallCount() >= ctx.getLimits().maxToolCalls()) {
            return Optional.of(new Violation("TOOL_LIMIT",
                    "超过最大工具调用次数 " + ctx.getLimits().maxToolCalls()));
        }
        if (ctx.remainingMs() <= 0) {
            return Optional.of(new Violation("WALL_CLOCK_TIMEOUT",
                    "超过最大执行时长 " + ctx.getLimits().maxWallClockMs() + "ms"));
        }
        if (ctx.budgetExhausted()) {
            return Optional.of(new Violation("TOKEN_BUDGET",
                    "超过 token 预算 " + ctx.getLimits().tokenBudget()));
        }
        return Optional.empty();
    }

    /** 同一工具 + 同一参数 连续出现 2 次即判定死循环 */
    public Optional<Violation> checkRepeat(RunContext ctx, String toolName, String argsJson) {
        String fp = sha256(toolName + "|" + normalize(argsJson));
        if (ctx.fingerprint(fp) >= 2) {
            return Optional.of(new Violation("LOOP_DETECTED",
                    "检测到重复调用工具 " + toolName));
        }
        return Optional.empty();
    }

    private String normalize(String argsJson) {
        // 去空白，避免同一参数因空格不同绕过检测
        return argsJson == null ? "" : argsJson.replaceAll("\\s+", "");
    }

    private String sha256(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }
}
