package com.gpb.datafirewall.services;

import static com.gpb.datafirewall.validation.DetailsTemplateValues.ERROR;
import static com.gpb.datafirewall.validation.DetailsTemplateValues.SUCCESS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import com.gpb.datafirewall.model.Rule;
import com.gpb.datafirewall.validation.ValidationResult;
import com.gpb.datafirewall.validation.ValidationMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ValidationService {

    private static final Logger log =
            LoggerFactory.getLogger(ValidationService.class);

    private static final String WARNING = "WARNING";

    private static final String CLASSIFICATION = "classification";
    private static final String CLASSIFICATION_INFORM = "INFORM";

    private static final String ERROR_DESCRIPTION = "errorDescription";

    public ValidationResult validate(
            Map<String, Rule> compiledRules,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRuleIds
    ) {
        return validate(
                compiledRules,
                normalizedMap,
                fieldToRuleIds,
                Map.of(),
                false
        );
    }

    /**
     * Совместимый вход без справочника сообщений об ошибках.
     */
    public ValidationResult validate(
            Map<String, Rule> compiledRules,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRuleIds,
            Boolean filterFlag
    ) {
        return validate(
                compiledRules,
                normalizedMap,
                fieldToRuleIds,
                Map.of(),
                filterFlag
        );
    }

    public ValidationResult validate(
            Map<String, Rule> compiledRules,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRuleIds,
            Map<String, Map<String, String>> errorMessagesByRule
    ) {
        return validate(
                compiledRules,
                normalizedMap,
                fieldToRuleIds,
                errorMessagesByRule,
                false
        );
    }

    public ValidationResult validate(
            Map<String, Rule> compiledRules,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRuleIds,
            Map<String, Map<String, String>> errorMessagesByRule,
            Boolean filterFlag
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

        final boolean debugEnabled = log.isDebugEnabled();

        if (debugEnabled) {
            log.debug(
                    "[RULE-DIAG] validate START compiledRules={} normalizedFields={} fieldToRules.fields={} fieldToRules.ruleRefs={}",
                    compiledRules.size(),
                    normalizedMap.size(),
                    fieldToRuleIds.size(),
                    countRuleReferences(fieldToRuleIds)
            );
        }

        boolean anyError = false;
        boolean anyWarning = false;
        boolean anyException = false;

        // logicalField -> (ruleName -> status)
        Map<String, Map<String, String>> detailByField =
                new LinkedHashMap<>();

        // logicalField -> [errorMessage1, errorMessage2, ...]
        Map<String, List<String>> errorsByField =
                new LinkedHashMap<>();

        Map<String, Set<String>> sortedFields = isNaturallyOrdered(fieldToRuleIds)
                ? fieldToRuleIds
                : new TreeMap<>(fieldToRuleIds);

        Map<String, String> ruleInput = normalizedMap;

        for (Map.Entry<String, Set<String>> entry : sortedFields.entrySet()) {

            String logicalField = entry.getKey();

            if (logicalField == null || logicalField.isBlank()) {
                continue;
            }

            Set<String> ruleNames = entry.getValue();

            if (ruleNames == null || ruleNames.isEmpty()) {
                continue;
            }

            Map<String, String> perRules =
                    new LinkedHashMap<>();

            Set<String> perFieldErrors = null;

            Iterable<String> sortedRuleNames;
            if (ruleNames instanceof SortedSet<?> sortedSet
                    && sortedSet.comparator() == null) {
                sortedRuleNames = ruleNames;
            } else {
                List<String> names = new ArrayList<>(ruleNames);
                names.sort(Comparator.naturalOrder());
                sortedRuleNames = names;
            }

            if (debugEnabled) {
                log.debug(
                        "[RULE-DIAG] FIELD field='{}' containsKey={} inputState={} configuredRules={}",
                        logicalField,
                        ruleInput.containsKey(logicalField),
                        valueState(ruleInput.get(logicalField)),
                        sortedRuleNames
                );
            }

            for (String ruleName : sortedRuleNames) {

                if (ruleName == null || ruleName.isBlank()) {
                    continue;
                }

                Rule rule =
                        resolveRule(
                                compiledRules,
                                ruleName
                        );

                /*
                 * Показываем:
                 * - какое поле проверяем;
                 * - какое правило;
                 * - нашли ли compiled rule;
                 * - конкретный класс правила;
                 * - какое состояние значения пришло в правило.
                 */
                if (debugEnabled) {
                    log.debug(
                            "[RULE-DIAG] BEFORE field='{}' rule={} resolved={} implClass={} containsKey={} inputState={}",
                            logicalField,
                            ruleName,
                            rule != null,
                            rule == null
                                    ? null
                                    : rule.getClass().getName(),
                            ruleInput.containsKey(logicalField),
                            valueState(ruleInput.get(logicalField))
                    );
                }

                boolean triggered;

                try {
                    // true = правило сработало = ошибка найдена.
                    triggered =
                            rule != null
                                    && rule.apply(ruleInput);

                    if (debugEnabled) {
                        log.debug(
                                "[RULE-DIAG] APPLY field='{}' rule={} returnedTriggered={}",
                                logicalField,
                                ruleName,
                                triggered
                        );
                    }

                } catch (NoSuchElementException ignored) {
                    if (Boolean.TRUE.equals(filterFlag)) {
                        // При включённой фильтрации проверка с отсутствующей
                        // зависимостью полностью исключается из результата.
                        continue;
                    }

                    // Отсутствующая зависимость является штатным SUCCESS,
                    // поэтому processStatus остаётся OK. Stack trace намеренно
                    // не передаётся в лог.
                    triggered = false;
                    log.warn(
                            "[RULE] missing field: rule={} fields={}",
                            ruleName,
                            referencedFieldsForLog(rule, logicalField)
                    );
                } catch (Exception ex) {

                    triggered = false;
                    anyException = true;

                    log.error(
                            "[RULE-DIAG] APPLY EXCEPTION field='{}' rule={} containsKey={} inputState={} implClass={}",
                            logicalField,
                            ruleName,
                            ruleInput.containsKey(logicalField),
                            valueState(ruleInput.get(logicalField)),
                            rule == null
                                    ? null
                                    : rule.getClass().getName(),
                            ex
                    );
                }

                String status = SUCCESS;
                Map<String, String> errorInfo = null;
                boolean errorInfoResolved = false;

                if (triggered) {

                    errorInfo = resolveErrorInfo(
                            ruleName,
                            errorMessagesByRule
                    );
                    errorInfoResolved = true;

                    status =
                            resolveTriggeredStatus(
                                    errorInfo
                            );

                    if (ERROR.equals(status)) {

                        anyError = true;

                    } else if (WARNING.equals(status)) {

                        anyWarning = true;
                    }

                    if (perFieldErrors == null) {
                        perFieldErrors = new LinkedHashSet<>();
                    }

                    perFieldErrors.add(
                            resolveErrorMessage(
                                    ruleName,
                                    errorInfo
                            )
                    );
                }

                /*
                 * Финальная диагностика конкретного правила.
                 */
                if (debugEnabled) {
                    if (!errorInfoResolved) {
                        errorInfo = resolveErrorInfo(
                                ruleName,
                                errorMessagesByRule
                        );
                    }

                    log.debug(
                            "[RULE-DIAG] AFTER field='{}' rule={} resolved={} triggered={} status={} classification={}",
                            logicalField,
                            ruleName,
                            rule != null,
                            triggered,
                            status,
                            errorInfo == null
                                    ? null
                                    : errorInfo.get(CLASSIFICATION)
                    );
                }

                /*
                 * Сейчас отсутствующее compiled rule автоматически
                 * приводит к triggered=false и далее SUCCESS.
                 *
                 * Пока бизнес-логику не меняем,
                 * но явно фиксируем эту ситуацию.
                 */
                if (rule == null) {

                    log.warn(
                            "[RULE-DIAG] RULE NOT FOUND field='{}' rule={} -> current logic produces SUCCESS",
                            logicalField,
                            ruleName
                    );
                }

                perRules.put(
                        ruleName,
                        status
                );
            }

            if (!perRules.isEmpty()) {

                detailByField.put(
                        logicalField,
                        Collections.unmodifiableMap(perRules)
                );
            }

            if (perFieldErrors != null && !perFieldErrors.isEmpty()) {

                errorsByField.put(
                        logicalField,
                        Collections.unmodifiableList(
                                new ArrayList<>(perFieldErrors)
                        )
                );
            }
        }

        String all =
                anyError
                        ? ERROR
                        : (
                        anyWarning
                                ? WARNING
                                : SUCCESS
                );

        String processStatus =
                anyException
                        ? "RULE_EXCEPTION"
                        : "OK";

        if (debugEnabled) {
            log.debug(
                    "[RULE-DIAG] validate END allResult={} processStatus={} detailFields={} errorFields={}",
                    all,
                    processStatus,
                    detailByField.size(),
                    errorsByField.size()
            );
        }

        return new ValidationResult(
                null,
                all,
                processStatus,
                Collections.unmodifiableMap(detailByField),
                Collections.emptyMap(),
                Collections.unmodifiableMap(errorsByField)
        );
    }

    /**
     * Отдельные разделы для короткого ответа MQ. Не повторяет выполнение правил:
     * используется статус из detailByField / detailByDataset и тот же справочник
     * сообщений, который применялся во время validate().
     */
    public ValidationMessages classifyMessages(
            ValidationResult validation,
            Map<String, Map<String, String>> errorMessagesByRule
    ) {
        if (validation == null) {
            return new ValidationMessages(Map.of(), Map.of(), Set.of());
        }

        Map<String, LinkedHashSet<String>> errors = new LinkedHashMap<>();
        Map<String, LinkedHashSet<String>> warnings = new LinkedHashMap<>();
        Map<String, Map<String, String>> messages =
                errorMessagesByRule == null ? Map.of() : errorMessagesByRule;
        Set<String> instances = new LinkedHashSet<>();

        appendClassifiedMessages(validation.detailByField(), "", messages, errors, warnings);

        // Сохраняем адресацию сообщений по конкретному элементу массива.
        // В detailByDataset также есть обычные datasetCode, их здесь пропускаем.
        if (validation.detailByDataset() != null) {
            for (Map.Entry<String, Map<String, Map<String, String>>> entry
                    : validation.detailByDataset().entrySet()) {
                String instanceKey = entry.getKey();
                if (!isArrayInstanceKey(instanceKey)) continue;
                instances.add(instanceKey);
                appendClassifiedMessages(
                        entry.getValue(), instanceKey + "::", messages, errors, warnings);
            }
        }

        return new ValidationMessages(
                freezeClassifiedMessages(errors),
                freezeClassifiedMessages(warnings),
                Collections.unmodifiableSet(instances)
        );
    }

    private void appendClassifiedMessages(
            Map<String, Map<String, String>> byField,
            String prefix,
            Map<String, Map<String, String>> errorMessagesByRule,
            Map<String, LinkedHashSet<String>> errors,
            Map<String, LinkedHashSet<String>> warnings
    ) {
        if (byField == null || byField.isEmpty()) return;

        for (Map.Entry<String, Map<String, String>> fieldEntry : byField.entrySet()) {
            String field = fieldEntry.getKey();
            Map<String, String> rules = fieldEntry.getValue();
            if (field == null || field.isBlank() || rules == null) continue;

            for (Map.Entry<String, String> ruleEntry : rules.entrySet()) {
                String rule = ruleEntry.getKey();
                String status = ruleEntry.getValue();
                if (rule == null || rule.isBlank()) continue;

                Map<String, LinkedHashSet<String>> destination;
                if (ERROR.equalsIgnoreCase(status)) {
                    destination = errors;
                } else if (WARNING.equalsIgnoreCase(status)) {
                    destination = warnings;
                } else {
                    continue; // SUCCESS и отсутствующие статусы не имеют сообщений.
                }

                String message = resolveErrorMessage(
                        rule, resolveErrorInfo(rule, errorMessagesByRule));
                destination.computeIfAbsent(prefix + field, k -> new LinkedHashSet<>())
                        .add(message);
            }
        }
    }

    private Map<String, List<String>> freezeClassifiedMessages(
            Map<String, LinkedHashSet<String>> source
    ) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        source.forEach((key, values) -> out.put(key, List.copyOf(values)));
        return Collections.unmodifiableMap(out);
    }

    private boolean isArrayInstanceKey(String key) {
        if (key == null) return false;
        int end = key.lastIndexOf('#');
        int first = key.lastIndexOf('#', end - 1);
        if (first < 0 || end <= first + 1 || end == key.length() - 1) return false;
        try {
            return Integer.parseInt(key.substring(end + 1)) >= 0;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private Set<String> referencedFieldsForLog(
            Rule rule,
            String anchorField
    ) {
        if (rule != null) {
            try {
                Set<String> fields = rule.referencedFields();
                if (fields != null && !fields.isEmpty()) {
                    return fields;
                }
            } catch (RuntimeException ignored) {
                // Диагностический код не должен менять результат проверки.
            }
        }

        return anchorField == null || anchorField.isBlank()
                ? Set.of()
                : Set.of(anchorField);
    }

    private String resolveErrorMessage(
            String ruleName,
            Map<String, String> errorInfo
    ) {

        if (ruleName == null
                || ruleName.isBlank()) {

            return "Не найден текст ошибки для неизвестного правила";
        }

        String errorDescr =
                extractErrorDescr(
                        errorInfo
                );

        if (errorDescr != null
                && !errorDescr.isBlank()) {

            return errorDescr;
        }

        return "Не найден текст ошибки для " + ruleName;
    }

    private String extractErrorDescr(
            Map<String, String> errorInfo
    ) {

        if (errorInfo == null
                || errorInfo.isEmpty()) {

            return null;
        }

        String value =
                errorInfo.get(ERROR_DESCRIPTION);

        return value == null
                || value.isBlank()
                ? null
                : value;
    }

    private Rule resolveRule(
            Map<String, Rule> compiledRules,
            String ruleName
    ) {
        return RuleIdLookup.resolve(compiledRules, ruleName);
    }

    private Map<String, String> resolveErrorInfo(
            String ruleName,
            Map<String, Map<String, String>> errorMessagesByRule
    ) {

        if (ruleName == null
                || ruleName.isBlank()
                || errorMessagesByRule == null
                || errorMessagesByRule.isEmpty()) {

            return null;
        }

        Map<String, String> direct =
                errorMessagesByRule.get(ruleName);

        if (direct != null
                && !direct.isEmpty()) {

            return direct;
        }

        if (!ruleName.startsWith("Rule")) {

            Map<String, String> prefixed =
                    errorMessagesByRule.get(
                            "Rule" + ruleName
                    );

            if (prefixed != null
                    && !prefixed.isEmpty()) {

                return prefixed;
            }
        }

        if (ruleName.startsWith("Rule")
                && ruleName.length() > 4) {

            Map<String, String> plain =
                    errorMessagesByRule.get(
                            ruleName.substring(4)
                    );

            if (plain != null
                    && !plain.isEmpty()) {

                return plain;
            }
        }

        return null;
    }

    private String resolveTriggeredStatus(
            Map<String, String> errorInfo
    ) {
        String classification =
                errorInfo == null
                        ? null
                        : errorInfo.get(CLASSIFICATION);

        if (classification != null
                && CLASSIFICATION_INFORM.equalsIgnoreCase(
                classification.trim()
        )) {

            return WARNING;
        }

        return ERROR;
    }

    private static boolean isNaturallyOrdered(
            Map<String, Set<String>> fields
    ) {
        if (fields == null || fields.isEmpty()) {
            return true;
        }

        if (fields instanceof SortedMap<?, ?> sortedMap
                && sortedMap.comparator() == null) {
            return true;
        }

        String previous = null;
        boolean hasPrevious = false;

        for (String current : fields.keySet()) {
            // TreeMap с natural order, который использовался раньше,
            // также не допускает null-ключи.
            if (current == null) {
                return false;
            }

            if (hasPrevious && previous.compareTo(current) > 0) {
                return false;
            }

            previous = current;
            hasPrevious = true;
        }

        return true;
    }

    private static int countRuleReferences(
            Map<String, Set<String>> fieldToRules
    ) {

        if (fieldToRules == null
                || fieldToRules.isEmpty()) {

            return 0;
        }

        int count = 0;

        for (Set<String> rules : fieldToRules.values()) {

            if (rules != null) {
                count += rules.size();
            }
        }

        return count;
    }

    /**
     * Значение специально не выводим в лог.
     * Показываем только его состояние.
     */
    private static String valueState(
            String value
    ) {

        if (value == null) {
            return "NULL";
        }

        if (value.isBlank()) {
            return "BLANK";
        }

        if ("none".equalsIgnoreCase(
                value.trim()
        )) {
            return "NONE";
        }

        return "PRESENT";
    }
}
