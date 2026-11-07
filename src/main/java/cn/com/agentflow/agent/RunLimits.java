package cn.com.agentflow.agent;

public record RunLimits(int maxSteps, int maxToolCalls, long maxWallClockMs, int tokenBudget) {
}
