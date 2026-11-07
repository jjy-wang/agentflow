package cn.com.agentflow.consumer;

import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 消费失败决策：只有明确可重试的错误才重试，重试上限由 agentflow.report.max-retry 控制。
 * 重试在原消费线程同步完成，不重新入队，避免 requeue 热循环把队列打满。
 */
@Component
@RequiredArgsConstructor
public class ReportRetryPolicy {

    private final ReportTaskProperties properties;

    public enum Decision {
        /** 还有重试额度，立即再试 */
        RETRY,
        /** 可重试错误但额度耗尽，标 FAILED 后进入死信 */
        DEAD_LETTER,
        /** 不可重试错误，标 FAILED 后 ACK */
        FAIL_FAST
    }

    /**
     * @param retriesDone 已经完成的重试次数（首次执行为 0）
     * @param error       本次执行的异常
     */
    public Decision decide(int retriesDone, Throwable error) {
        if (!isRetryable(error)) {
            return Decision.FAIL_FAST;
        }
        return retriesDone < maxRetry() ? Decision.RETRY : Decision.DEAD_LETTER;
    }

    /** 首次失败后允许的最大重试次数，负数按 0 处理 */
    public int maxRetry() {
        return Math.max(0, properties.getMaxRetry());
    }

    /** 临时 DB 异常、下游 5xx、文件/网络 IO 可重试；参数与数据不存在等业务错误不可重试 */
    static boolean isRetryable(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof RetryableException) {
                return true;
            }
            if (current instanceof DataAccessException) {
                return true;
            }
            if (current instanceof IOException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /** 下游 5xx/临时不可用等可重试异常，由报表生成实现按需抛出 */
    public static class RetryableException extends RuntimeException {
        public RetryableException(String message) {
            super(message);
        }

        public RetryableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
