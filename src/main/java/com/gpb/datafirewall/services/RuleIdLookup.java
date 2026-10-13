package com.gpb.datafirewall.services;

import java.util.Map;

/**
 * Единая логика разрешения идентификатора правила.
 *
 * <p>Politics-кэш может содержать как {@code 10220}, так и
 * {@code Rule10220}. И ValidationService, и построитель индекса зависимостей
 * должны разрешать эти варианты одинаково.</p>
 */
final class RuleIdLookup {

    private static final String RULE_PREFIX = "Rule";

    private RuleIdLookup() {
    }

    static <T> T resolve(Map<String, T> valuesByRuleId, String ruleId) {
        if (valuesByRuleId == null
                || valuesByRuleId.isEmpty()
                || ruleId == null
                || ruleId.isBlank()) {
            return null;
        }

        T value = valuesByRuleId.get(ruleId);
        if (value != null) {
            return value;
        }

        if (!ruleId.startsWith(RULE_PREFIX)) {
            return valuesByRuleId.get(RULE_PREFIX + ruleId);
        }

        if (ruleId.length() > RULE_PREFIX.length()) {
            return valuesByRuleId.get(ruleId.substring(RULE_PREFIX.length()));
        }

        return null;
    }
}
