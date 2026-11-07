package cn.com.agentflow.audit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** SensitiveMasker：手机号/身份证/邮箱/sk- 密钥/k=v 凭据的统一脱敏规则 */
class SensitiveMaskerTest {

    private final SensitiveMasker masker = new SensitiveMasker();

    @Test
    void 手机号保留前3后4() {
        assertEquals("联系电话 138****5678", masker.mask("联系电话 13812345678"));
    }

    @Test
    void 身份证保留前3后4中间11位星号() {
        assertEquals("110***********1234", masker.mask("110101199001011234"));
    }

    @Test
    void 邮箱保留本地部分前3位() {
        assertEquals("zha***@example.com", masker.mask("zhangsan@example.com"));
    }

    @Test
    void sk前缀密钥整体打码() {
        assertEquals("密钥: sk-***", masker.mask("密钥: sk-abcdefgh12345"));
    }

    @Test
    void kv形式凭据值整体打码且键保留() {
        assertEquals("password=***", masker.mask("password=hunter2"));
        assertEquals("token=***", masker.mask("token=abc123xyz"));
    }

    @Test
    void kv打码优先于手机号规则() {
        // token 中的数字段不得被手机号规则二次破坏
        assertEquals("token=***", masker.mask("token=13812345678"));
    }

    @Test
    void null与空串原样返回() {
        assertNull(masker.mask(null));
        assertEquals("", masker.mask(""));
    }

    @Test
    void 普通文本不受影响() {
        assertEquals("帮我查一下资产", masker.mask("帮我查一下资产"));
    }
}
