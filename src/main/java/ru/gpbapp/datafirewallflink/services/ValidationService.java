package ru.gpbapp.datafirewallflink.services;

import com.gpb.datafirewall.model.Rule;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.ERROR;
import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.SUCCESS;

public final class ValidationService {

    public ValidationResult validate(
            Map<String, Rule> compiledRules,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRuleIds
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

        boolean anyError = false;
        boolean anyException = false;

        // logicalField -> (ruleName -> status)
        Map<String, Map<String, String>> detailByField = new LinkedHashMap<>();

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

            List<String> sortedRuleNames = new ArrayList<>(ruleNames);
            sortedRuleNames.sort(Comparator.naturalOrder());

            for (String ruleName : sortedRuleNames) {
                if (ruleName == null || ruleName.isBlank()) {
                    continue;
                }

                Rule rule = resolveRule(compiledRules, ruleName);

                boolean triggered;
                try {
                    triggered = rule != null && rule.apply(normalizedMap);
                } catch (Exception ex) {
                    triggered = false;
                    anyException = true;
                }

                String status = triggered ? ERROR : SUCCESS;
                if (triggered) {
                    anyError = true;
                }

                perRules.put(ruleName, status);
            }

            if (!perRules.isEmpty()) {
                detailByField.put(logicalField, Collections.unmodifiableMap(perRules));
            }
        }

        String all = anyError ? ERROR : SUCCESS;
        String processStatus = anyException ? "RULE_EXCEPTION" : "OK";

        return new ValidationResult(
                null,
                all,
                processStatus,
                Collections.unmodifiableMap(detailByField),
                Collections.emptyMap()
        );
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