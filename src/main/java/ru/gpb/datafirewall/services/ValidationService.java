package ru.gpb.datafirewall.services;

import static ru.gpb.datafirewall.validation.DetailsTemplateValues.ERROR;
import static ru.gpb.datafirewall.validation.DetailsTemplateValues.SUCCESS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import ru.gpb.datafirewall.dto.Rule;
import ru.gpb.datafirewall.validation.ValidationResult;

public final class ValidationService {

    public ValidationResult validate(
            Map<String, Rule> compiledRules,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRuleIds
    ) {
        return validate(compiledRules, normalizedMap, fieldToRuleIds, Map.of());
    }

    public ValidationResult validate(
            Map<String, Rule> compiledRules,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRuleIds,
            Map<String, String> errorMessagesByRule
    ) {
        if (compiledRules == null) {
            compiledRules = Map.of();
        }
        if (normalizedMap == null) {
            normalizedMap = Map.of();
        }
        if (fieldToRuleIds == null) {
            fieldToRuleIds = Map.of();
        }
        if (errorMessagesByRule == null) {
            errorMessagesByRule = Map.of();
        }

        boolean anyError = false;
        boolean anyException = false;

        // logicalField -> (ruleName -> status)
        Map<String, Map<String, String>> detailByField = new LinkedHashMap<>();

        // logicalField -> [errorMessage1, errorMessage2, ...]
        Map<String, List<String>> errorsByField = new LinkedHashMap<>();

        Map<String, Set<String>> sortedFields = new TreeMap<>(fieldToRuleIds);

        for (Map.Entry<String, Set<String>> entry : sortedFields.entrySet()) {
            String logicalField = entry.getKey();
            if (logicalField == null || logicalField.isBlank()) {
                continue;
            }

            Set<String> ruleNames = entry.getValue();
            if (ruleNames == null || ruleNames.isEmpty()) {
                continue;
            }

            Map<String, String> perRules = new LinkedHashMap<>();
            Set<String> perFieldErrors = new LinkedHashSet<>();

            List<String> sortedRuleNames = new ArrayList<>(ruleNames);
            sortedRuleNames.sort(Comparator.naturalOrder());

            for (String ruleName : sortedRuleNames) {
                if (ruleName == null || ruleName.isBlank()) {
                    continue;
                }

                Rule rule = resolveRule(compiledRules, ruleName);

                boolean triggered;
                try {
                    // true = правило сработало = ошибка найдена
                    triggered = rule != null && rule.apply(normalizedMap);
                } catch (Exception ex) {
                    triggered = false;
                    anyException = true;
                }

                String status = triggered ? ERROR : SUCCESS;
                if (triggered) {
                    anyError = true;
                    perFieldErrors.add(resolveErrorMessage(ruleName, errorMessagesByRule));
                }

                perRules.put(ruleName, status);
            }

            if (!perRules.isEmpty()) {
                detailByField.put(logicalField, Collections.unmodifiableMap(perRules));
            }

            if (!perFieldErrors.isEmpty()) {
                errorsByField.put(
                        logicalField,
                        Collections.unmodifiableList(new ArrayList<>(perFieldErrors))
                );
            }
        }

        String all = anyError ? ERROR : SUCCESS;
        String processStatus = anyException ? "RULE_EXCEPTION" : "OK";

        return new ValidationResult(
                null,
                all,
                processStatus,
                Collections.unmodifiableMap(detailByField),
                Collections.emptyMap(),
                Collections.unmodifiableMap(errorsByField)
        );
    }

    private String resolveErrorMessage(
            String ruleName,
            Map<String, String> errorMessagesByRule
    ) {
        if (ruleName == null || ruleName.isBlank()) {
            return "Не найден текст ошибки для неизвестного правила";
        }

        String direct = errorMessagesByRule.get(ruleName);
        if (direct != null && !direct.isBlank()) {
            return direct;
        }

        if (!ruleName.startsWith("Rule")) {
            String prefixed = errorMessagesByRule.get("Rule" + ruleName);
            if (prefixed != null && !prefixed.isBlank()) {
                return prefixed;
            }
        }

        if (ruleName.startsWith("Rule") && ruleName.length() > 4) {
            String plain = errorMessagesByRule.get(ruleName.substring(4));
            if (plain != null && !plain.isBlank()) {
                return plain;
            }
        }

        return "Не найден текст ошибки для " + ruleName;
    }

    private Rule resolveRule(Map<String, Rule> compiledRules, String ruleName) {
        Rule rule = compiledRules.get(ruleName);
        if (rule != null) {
            return rule;
        }

        if (!ruleName.startsWith("Rule")) {
            rule = compiledRules.get("Rule" + ruleName);
            if (rule != null) {
                return rule;
            }
        }

        if (ruleName.startsWith("Rule") && ruleName.length() > 4) {
            return compiledRules.get(ruleName.substring(4));
        }

        return null;
    }
}