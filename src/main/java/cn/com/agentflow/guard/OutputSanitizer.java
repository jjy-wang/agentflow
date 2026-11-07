package cn.com.agentflow.guard;

import cn.com.agentflow.audit.SensitiveMasker;
import org.springframework.stereotype.Component;

/** 输出净化：模型输出对外返回前的统一处理，目前做敏感信息脱敏 */
@Component
public class OutputSanitizer {

    private final SensitiveMasker masker;

    public OutputSanitizer(SensitiveMasker masker) {
        this.masker = masker;
    }

    public String sanitize(String text) {
        return text == null ? null : masker.mask(text);
    }
}
