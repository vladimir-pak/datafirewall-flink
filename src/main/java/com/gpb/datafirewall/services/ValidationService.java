package com.gpb.datafirewall.services;

import static com.gpb.datafirewall.validation.DetailsTemplateValues.ERROR;
import static com.gpb.datafirewall.validation.DetailsTemplateValues.SUCCESS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import com.gpb.datafirewall.enums.DocType;
import com.gpb.datafirewall.enums.Gender;
import com.gpb.datafirewall.model.Rule;
import com.gpb.datafirewall.validation.ValidationResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ValidationService {

    private static final Logger log =
            LoggerFactory.getLogger(ValidationService.class);

    private static final Map<String, Function<String, String>> mappingAttributes = Map.of(
            "ОСНОВНЫЕ СВЕДЕНИЯ.Пол", Gender::map,
            "ДУЛ.Паспорт РФ.Тип ДУЛ", DocType::map
    );

    private static final String WARNING = "WARNING";

    private static final String CLASSIFICATION = "classification";
    private static final String CLASSIFICATION_INFORM = "INFORM";

    private static final String ERROR_DESCRIPTION = "errorDescription";

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

        log.info(
                "[RULE-DIAG] validate START compiledRules={} normalizedFields={} fieldToRules.fields={} fieldToRules.ruleRefs={}",
                compiledRules.size(),
                normalizedMap.size(),
                fieldToRuleIds.size(),
                countRuleReferences(fieldToRuleIds)
        );

        boolean anyError = false;
        boolean anyWarning = false;
        boolean anyException = false;

        // logicalField -> (ruleName -> status)
        Map<String, Map<String, String>> detailByField =
                new LinkedHashMap<>();

        // logicalField -> [errorMessage1, errorMessage2, ...]
        Map<String, List<String>> errorsByField =
                new LinkedHashMap<>();

        Map<String, Set<String>> sortedFields =
                new TreeMap<>(fieldToRuleIds);

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

            Set<String> perFieldErrors =
                    new LinkedHashSet<>();

            List<String> sortedRuleNames =
                    new ArrayList<>(ruleNames);

            sortedRuleNames.sort(
                    Comparator.naturalOrder()
            );

            Map<String, String> ruleInput =
                    mapLogicalFieldValueIfRequired(
                            logicalField,
                            normalizedMap
                    );

            log.info(
                    "[RULE-DIAG] FIELD field='{}' containsKey={} inputState={} configuredRules={}",
                    logicalField,
                    ruleInput.containsKey(logicalField),
                    valueState(ruleInput.get(logicalField)),
                    sortedRuleNames
            );

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
                log.info(
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

                boolean triggered;

                try {
                    /*
                     * true = правило сработало = ошибка найдена.
                     *
                     * ВАЖНО:
                     * текущую бизнес-логику здесь НЕ меняем.
                     */
                    triggered =
                            rule != null
                                    && rule.apply(ruleInput);

                    log.info(
                            "[RULE-DIAG] APPLY field='{}' rule={} returnedTriggered={}",
                            logicalField,
                            ruleName,
                            triggered
                    );

                } catch (NoSuchElementException ex) {
                    triggered = false;
                    anyException = true;

                    if (Boolean.TRUE.equals(filterFlag)) {
                        continue;
                    }

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

                if (triggered) {

                    status =
                            resolveTriggeredStatus(
                                    ruleName,
                                    errorMessagesByRule
                            );

                    if (ERROR.equals(status)) {

                        anyError = true;

                    } else if (WARNING.equals(status)) {

                        anyWarning = true;
                    }

                    perFieldErrors.add(
                            resolveErrorMessage(
                                    ruleName,
                                    errorMessagesByRule
                            )
                    );
                }

                Map<String, String> errorInfo =
                        resolveErrorInfo(
                                ruleName,
                                errorMessagesByRule
                        );

                /*
                 * Финальная диагностика конкретного правила.
                 */
                log.info(
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

            if (!perFieldErrors.isEmpty()) {

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

        log.info(
                "[RULE-DIAG] validate END allResult={} processStatus={} detailFields={} errorFields={}",
                all,
                processStatus,
                detailByField.size(),
                errorsByField.size()
        );

        return new ValidationResult(
                null,
                all,
                processStatus,
                Collections.unmodifiableMap(detailByField),
                Collections.emptyMap(),
                Collections.unmodifiableMap(errorsByField)
        );
    }

    private Map<String, String> mapLogicalFieldValueIfRequired(
            String logicalField,
            Map<String, String> normalizedMap
    ) {

        if (logicalField == null
                || logicalField.isBlank()
                || normalizedMap == null
                || normalizedMap.isEmpty()) {

            return normalizedMap;
        }

        Function<String, String> mapper =
                mappingAttributes.get(logicalField);

        if (mapper == null
                || !normalizedMap.containsKey(logicalField)) {

            return normalizedMap;
        }

        String currentValue =
                normalizedMap.get(logicalField);

        if (currentValue == null
                || currentValue.isBlank()) {

            return normalizedMap;
        }

        String normalizedValue =
                currentValue.trim();

        String mappedValue =
                mapper.apply(normalizedValue);

        if (mappedValue == null) {

            mappedValue =
                    mapper.apply(
                            normalizedValue.toUpperCase(
                                    Locale.ROOT
                            )
                    );
        }

        if (mappedValue == null
                || mappedValue.equals(currentValue)) {

            return normalizedMap;
        }

        Map<String, String> mapped =
                new LinkedHashMap<>(normalizedMap);

        mapped.put(
                logicalField,
                mappedValue
        );

        return Collections.unmodifiableMap(mapped);
    }

    private String resolveErrorMessage(
            String ruleName,
            Map<String, Map<String, String>> errorMessagesByRule
    ) {

        if (ruleName == null
                || ruleName.isBlank()) {

            return "Не найден текст ошибки для неизвестного правила";
        }

        String errorDescr =
                extractErrorDescr(
                        resolveErrorInfo(
                                ruleName,
                                errorMessagesByRule
                        )
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

        Rule rule =
                compiledRules.get(ruleName);

        if (rule != null) {
            return rule;
        }

        if (!ruleName.startsWith("Rule")) {

            rule =
                    compiledRules.get(
                            "Rule" + ruleName
                    );

            if (rule != null) {
                return rule;
            }
        }

        if (ruleName.startsWith("Rule")
                && ruleName.length() > 4) {

            return compiledRules.get(
                    ruleName.substring(4)
            );
        }

        return null;
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
            String ruleName,
            Map<String, Map<String, String>> errorMessagesByRule
    ) {

        Map<String, String> errorInfo =
                resolveErrorInfo(
                        ruleName,
                        errorMessagesByRule
                );

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