package ru.gpbapp.datafirewallflink.services;

import com.gpb.datafirewall.model.Rule;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.*;

import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.ERROR;
import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.SUCCESS;

public final class ValidationService {

    public ValidationResult validate(Map<String, Rule> compiledRules,
                                     Map<String, String> normalizedMap,
                                     Map<String, Set<String>> fieldToRuleIds) {

        if (compiledRules == null) compiledRules = Map.of();
        if (normalizedMap == null) normalizedMap = Map.of();
        if (fieldToRuleIds == null) fieldToRuleIds = Map.of();

        boolean anyError = false;
        boolean anyException = false;

        // logicalField -> (ruleId -> status)
        Map<String, Map<String, String>> detailByField = new LinkedHashMap<>();

        // стабильность порядка (для логов/сравнений)
        Map<String, Set<String>> sortedFields = new TreeMap<>(fieldToRuleIds);

        for (Map.Entry<String, Set<String>> e : sortedFields.entrySet()) {
            String logicalField = e.getKey();
            if (logicalField == null || logicalField.isBlank()) continue;

            Set<String> ruleIds = e.getValue();
            if (ruleIds == null || ruleIds.isEmpty()) continue;

            Map<String, String> perRules = new LinkedHashMap<>();

            // сортируем ruleIds численно/лексикографически
            List<String> sortedRuleIds = new ArrayList<>(ruleIds);
            sortedRuleIds.sort(Comparator.naturalOrder());

            for (String ruleId : sortedRuleIds) {
                if (ruleId == null || ruleId.isBlank()) continue;

                // В compiledRules ключи обычно вида Rule1065, а в detail/results нужен просто 1065
                Rule rule = compiledRules.get(ruleId);
                if (rule == null) {
                    rule = compiledRules.get("Rule" + ruleId);
                }

                boolean triggered;
                try {
                    // true = правило сработало = найдена ошибка
                    triggered = (rule != null) && rule.apply(normalizedMap);
                } catch (Exception ex) {
                    triggered = false;
                    anyException = true;
                }

                String status = triggered ? ERROR : SUCCESS;
                if (triggered) anyError = true;

                perRules.put(ruleId, status);
            }

            if (!perRules.isEmpty()) {
                detailByField.put(logicalField, Collections.unmodifiableMap(perRules));
            }
        }

        String all = anyError ? ERROR : SUCCESS;
        String processStatus = anyException ? "RULE_EXCEPTION" : "OK";

        return new ValidationResult(null, all, processStatus, Collections.unmodifiableMap(detailByField));
    }
}