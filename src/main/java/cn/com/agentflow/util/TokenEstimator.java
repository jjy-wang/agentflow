package cn.com.agentflow.util;

/**
 * 轻量 token 估算：中文约 1.5 字/token，英文约 4 字符/token。
 * 生产可替换为真实 tokenizer；用于预算分配足够。
 */
public final class TokenEstimator {

    private TokenEstimator() {
    }

    public static int estimate(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        int cjk = 0;
        int other = 0;
        for (char c : text.toCharArray()) {
            if (c >= 0x4E00 && c <= 0x9FFF) {
                cjk++;
            } else {
                other++;
            }
        }
        return (int) Math.ceil(cjk / 1.5 + other / 4.0);
    }
}
