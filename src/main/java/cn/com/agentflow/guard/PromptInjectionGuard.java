package cn.com.agentflow.guard;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 提示词注入检测：中英双语特征正则，按最高命中严重度给出 HIGH/MEDIUM/LOW/NONE。
 * 只评估不拦截，拦截策略（拒绝/降级/仅记录）由调用方按 level 决定。
 */
@Component
public class PromptInjectionGuard {

    public enum Level {NONE, LOW, MEDIUM, HIGH}

    /** 评估结果：level 为最高命中等级，hits 为命中的特征描述 */
    public record Risk(Level level, List<String> hits) {

        public static Risk none() {
            return new Risk(Level.NONE, List.of());
        }
    }

    private record Rule(Pattern pattern, Level level, String label) {
    }

    private static final List<Rule> RULES = List.of(
            // —— HIGH：明确的指令覆写 / 越狱意图 ——
            new Rule(Pattern.compile("(?i)ignore\\s+(all\\s+|any\\s+|the\\s+)?(previous|above|prior|earlier)\\s+(instructions?|prompts?|rules?)"),
                    Level.HIGH, "ignore previous/above instructions"),
            new Rule(Pattern.compile("忽略(之前|以上|上述|前面|所有|全部)的?(指令|指示|提示|规则)"),
                    Level.HIGH, "忽略之前/以上/所有指令"),
            new Rule(Pattern.compile("(?i)reveal\\s+(your|the)\\s+(instructions?|system\\s+prompt|prompt)"),
                    Level.HIGH, "reveal your instructions"),
            new Rule(Pattern.compile("(?i)\\bjailbreak\\b"),
                    Level.HIGH, "jailbreak"),
            new Rule(Pattern.compile("(?i)\\bDAN\\b"),
                    Level.HIGH, "DAN"),
            new Rule(Pattern.compile("(?i)do\\s+anything\\s+now"),
                    Level.HIGH, "do anything now"),
            new Rule(Pattern.compile("(?i)forget\\s+(your|all|the)\\s+(instructions?|rules?|prompt)"),
                    Level.HIGH, "forget your instructions"),
            new Rule(Pattern.compile("忘记(你的|所有|全部|之前)的?(指令|规则|设定|提示)"),
                    Level.HIGH, "忘记指令/设定"),
            // —— MEDIUM：角色重定义 / 探查系统提示词 ——
            new Rule(Pattern.compile("(?i)\\bsystem\\s+prompt\\b"),
                    Level.MEDIUM, "system prompt"),
            new Rule(Pattern.compile("(?i)\\byou\\s+are\\s+now\\b"),
                    Level.MEDIUM, "you are now"),
            new Rule(Pattern.compile("(?i)\\bact\\s+as\\b"),
                    Level.MEDIUM, "act as"),
            new Rule(Pattern.compile("(?i)pretend\\s+(to\\s+be|you\\s+are)"),
                    Level.MEDIUM, "pretend to be"),
            new Rule(Pattern.compile("你(现在)?是(一个|一名|新的)"),
                    Level.MEDIUM, "你现在是…（角色重定义）"),
            // —— LOW：旁敲侧击的提示词套取的 ——
            new Rule(Pattern.compile("(?i)what\\s+(are|were)\\s+your\\s+(instructions?|rules?|prompt)"),
                    Level.LOW, "what are your instructions"),
            new Rule(Pattern.compile("(?i)(print|show|repeat|output)\\s+(your|the)\\s+(prompt|instructions?|system\\s+message)"),
                    Level.LOW, "print/show your prompt")
    );

    /** 评估输入文本的注入风险；空文本直接 NONE */
    public Risk assess(String text) {
        if (text == null || text.isBlank()) {
            return Risk.none();
        }
        List<String> hits = new ArrayList<>();
        Level level = Level.NONE;
        for (Rule rule : RULES) {
            if (rule.pattern().matcher(text).find()) {
                hits.add(rule.label());
                if (rule.level().ordinal() > level.ordinal()) {
                    level = rule.level();
                }
            }
        }
        return hits.isEmpty() ? Risk.none() : new Risk(level, List.copyOf(hits));
    }
}
