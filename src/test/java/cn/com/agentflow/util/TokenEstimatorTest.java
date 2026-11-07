package cn.com.agentflow.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TokenEstimator 估算规则：中文 1.5 字/token，英文 4 字符/token */
class TokenEstimatorTest {

    @Test
    void 空与null返回0() {
        assertEquals(0, TokenEstimator.estimate(null));
        assertEquals(0, TokenEstimator.estimate(""));
    }

    @Test
    void 纯中文按1点5字每token向上取整() {
        // 30 个 CJK 字符 → ceil(30/1.5) = 20
        assertEquals(20, TokenEstimator.estimate("数".repeat(30)));
        // 31 个 CJK 字符 → ceil(20.67) = 21
        assertEquals(21, TokenEstimator.estimate("数".repeat(31)));
    }

    @Test
    void 纯英文按4字符每token向上取整() {
        assertEquals(5, TokenEstimator.estimate("a".repeat(20)));
        assertEquals(6, TokenEstimator.estimate("a".repeat(21)));
    }

    @Test
    void 中英混排按各自汇率求和() {
        // 15 中文 → 10，8 英文 → 2，合计 12
        assertEquals(12, TokenEstimator.estimate("数".repeat(15) + "a".repeat(8)));
    }

    @Test
    void 估算结果单调非减() {
        int prev = 0;
        for (int i = 1; i <= 100; i++) {
            int cur = TokenEstimator.estimate("字".repeat(i));
            assertTrue(cur >= prev, "估算必须随长度单调非减");
            prev = cur;
        }
    }
}
