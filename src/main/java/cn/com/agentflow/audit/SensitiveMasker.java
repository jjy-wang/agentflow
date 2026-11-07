package cn.com.agentflow.audit;

import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

/**
 * 敏感信息脱敏：审计日志与模型输出对外前的统一处理。
 * 手机号 / 身份证保留前 3 后 4，邮箱保留本地部分前 3 位，其余整体替换为 ***。
 */
@Component
public class SensitiveMasker {

    /** 手机号 */
    private static final Pattern PHONE = Pattern.compile("1[3-9]\\d{9}");
    /** 18 位身份证号 */
    private static final Pattern ID_CARD = Pattern.compile("\\b\\d{17}[\\dXx]\\b");
    /** 邮箱 */
    private static final Pattern EMAIL = Pattern.compile("[\\w.+-]+@([\\w-]+\\.)+[\\w-]+");
    /** sk- 前缀的 API 密钥 */
    private static final Pattern API_SECRET = Pattern.compile("sk-[A-Za-z0-9]{8,}");
    /** password= / token= 等 k=v 形式的凭据 */
    private static final Pattern KV_SECRET =
            Pattern.compile("(?i)(password|passwd|token|secret|api[_-]?key)=([^\\s&]+)");

    public String mask(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        // k=v 优先：避免其中的数字段被手机号/身份证规则二次破坏
        String out = KV_SECRET.matcher(text).replaceAll("$1=***");
        out = API_SECRET.matcher(out).replaceAll("sk-***");
        out = ID_CARD.matcher(out).replaceAll(m -> {
            String g = m.group();
            return g.substring(0, 3) + "***********" + g.substring(14);
        });
        out = PHONE.matcher(out).replaceAll(m -> {
            String g = m.group();
            return g.substring(0, 3) + "****" + g.substring(7);
        });
        out = EMAIL.matcher(out).replaceAll(m -> {
            String g = m.group();
            int at = g.indexOf('@');
            String local = g.substring(0, at);
            return local.substring(0, Math.min(3, local.length())) + "***" + g.substring(at);
        });
        return out;
    }
}
