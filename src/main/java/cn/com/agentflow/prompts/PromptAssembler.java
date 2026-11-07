package cn.com.agentflow.prompts;

import cn.com.agentflow.llm.ToolDef;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * prompt 前缀稳定性工具：把"最稳定 → 最易变"的拼装约定收敛到一处。
 *
 * <p>稳定前缀 = 固定角色说明 + 按 name 字典序排列的工具 schema。
 * 记忆召回、会话摘要、计划、当前时间等动态段必须追加在稳定前缀之后，
 * 因此指纹只覆盖稳定前缀，动态内容变化不会让指纹漂移。</p>
 */
public final class PromptAssembler {

    /**
     * 动态段起始标记：出现即视为稳定前缀的结束位置。
     * 这些标记由记忆召回/计划续跑等链路追加在 system prompt 末尾，
     * 调整顺序时必须保持"稳定段在前、动态段在后"。
     */
    static final List<String> DYNAMIC_MARKERS = List.of(
            "\n\n【长期记忆·自动召回】",
            "\n\n【已确认的执行计划】",
            "\n\n【当前时间】");

    private static final int FINGERPRINT_HEX_LENGTH = 16;
    private static final byte FIELD_SEPARATOR = 0x1f;

    private PromptAssembler() {
    }

    /** 工具 schema 按 name 字典序稳定排序；null/空列表返回空列表 */
    public static List<ToolDef> sortTools(List<ToolDef> tools) {
        if (tools == null || tools.isEmpty()) {
            return List.of();
        }
        return tools.stream()
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(ToolDef::name, Comparator.nullsLast(String::compareTo)))
                .toList();
    }

    /** 截取稳定前缀：命中任一动态标记即截断，去掉尾部空白避免拼接差异 */
    public static String stablePrefix(String systemPrompt) {
        if (systemPrompt == null || systemPrompt.isEmpty()) {
            return "";
        }
        int cut = systemPrompt.length();
        for (String marker : DYNAMIC_MARKERS) {
            int index = systemPrompt.indexOf(marker);
            if (index >= 0 && index < cut) {
                cut = index;
            }
        }
        return systemPrompt.substring(0, cut).stripTrailing();
    }

    /**
     * 前缀指纹：sha256(稳定前缀 + 排序后的工具 schema) 取前 16 位十六进制。
     * 同一工具集的两次构建应完全一致，用于日志与"指纹复用"指标验证缓存可复用性。
     */
    public static String fingerprint(String systemPrompt, List<ToolDef> tools) {
        MessageDigest digest = sha256();
        digest.update(stablePrefix(systemPrompt).getBytes(StandardCharsets.UTF_8));
        for (ToolDef tool : sortTools(tools)) {
            digest.update(FIELD_SEPARATOR);
            digest.update(safe(tool.name()).getBytes(StandardCharsets.UTF_8));
            digest.update(FIELD_SEPARATOR);
            digest.update(safe(tool.description()).getBytes(StandardCharsets.UTF_8));
            digest.update(FIELD_SEPARATOR);
            digest.update(safe(tool.parameters() == null ? "" : tool.parameters().toString())
                    .getBytes(StandardCharsets.UTF_8));
        }
        return HexFormat.of().formatHex(digest.digest()).substring(0, FINGERPRINT_HEX_LENGTH);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK 缺少 SHA-256 实现", e);
        }
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }
}
