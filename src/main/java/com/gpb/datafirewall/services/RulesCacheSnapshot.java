package com.gpb.datafirewall.services;

import com.gpb.datafirewall.model.Rule;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Единый immutable snapshot всех кэшей, используемых при обработке сообщения.
 *
 * <p>Каждый входящий event получает одну ссылку на этот объект и до конца
 * обработки работает только с ней. Публикация новой версии выполняется одной
 * атомарной заменой ссылки в {@link RulesCacheRuntime}.</p>
 *
 * <p>Вместе с исходными кэшами snapshot содержит два производных индекса:</p>
 * <ul>
 *     <li>{@code ruleId -> referencedFields};</li>
 *     <li>{@code controlArea -> union(referencedFields)}.</li>
 * </ul>
 * Они строятся только при bootstrap/reload, а не для каждого сообщения.
 */
public final class RulesCacheSnapshot {

    private final Map<String, Rule> compiledRules;
    private final Map<String, String> dataset2ControlArea;
    private final Map<String, Map<String, Set<String>>> controlAreaRules;
    private final Map<String, Map<String, String>> errorMessagesByRule;
    private final Map<String, Set<String>> excludedBlocksByControlArea;
    private final Map<String, Boolean> filterFlagsByControlArea;
    private final Map<String, String> genderMapping;
    private final Map<String, String> typeDulMapping;
    private final String compiledRulesVersion;
    private final String politicsVersion;

    private final Map<String, Set<String>> referencedFieldsByRule;
    private final Map<String, Set<String>> referencedFieldsByControlArea;

    public RulesCacheSnapshot(
            Map<String, Rule> compiledRules,
            Map<String, String> dataset2ControlArea,
            Map<String, Map<String, Set<String>>> controlAreaRules,
            Map<String, Map<String, String>> errorMessagesByRule,
            Map<String, Set<String>> excludedBlocksByControlArea,
            Map<String, Boolean> filterFlagsByControlArea,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping,
            String compiledRulesVersion,
            String politicsVersion
    ) {
        this(
                compiledRules,
                dataset2ControlArea,
                controlAreaRules,
                errorMessagesByRule,
                excludedBlocksByControlArea,
                filterFlagsByControlArea,
                genderMapping,
                typeDulMapping,
                compiledRulesVersion,
                politicsVersion,
                null
        );
    }

    private RulesCacheSnapshot(
            Map<String, Rule> compiledRules,
            Map<String, String> dataset2ControlArea,
            Map<String, Map<String, Set<String>>> controlAreaRules,
            Map<String, Map<String, String>> errorMessagesByRule,
            Map<String, Set<String>> excludedBlocksByControlArea,
            Map<String, Boolean> filterFlagsByControlArea,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping,
            String compiledRulesVersion,
            String politicsVersion,
            Map<String, Set<String>> preparedReferencedFieldsByRule
    ) {
        this.compiledRules = safeMap(compiledRules);
        this.dataset2ControlArea = safeMap(dataset2ControlArea);
        this.controlAreaRules = safeMap(controlAreaRules);
        this.errorMessagesByRule = safeMap(errorMessagesByRule);
        this.excludedBlocksByControlArea = safeMap(excludedBlocksByControlArea);
        this.filterFlagsByControlArea = safeMap(filterFlagsByControlArea);
        this.genderMapping = safeMap(genderMapping);
        this.typeDulMapping = safeMap(typeDulMapping);
        this.compiledRulesVersion = compiledRulesVersion;
        this.politicsVersion = politicsVersion;

        this.referencedFieldsByRule = preparedReferencedFieldsByRule == null
                ? buildReferencedFieldsByRule(this.compiledRules)
                : preparedReferencedFieldsByRule;
        this.referencedFieldsByControlArea = buildReferencedFieldsByControlArea(
                this.controlAreaRules,
                this.referencedFieldsByRule
        );
    }

    public static RulesCacheSnapshot empty() {
        return new RulesCacheSnapshot(
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                null,
                null
        );
    }

    public Map<String, Rule> compiledRules() {
        return compiledRules;
    }

    public Map<String, String> dataset2ControlArea() {
        return dataset2ControlArea;
    }

    public Map<String, Map<String, Set<String>>> controlAreaRules() {
        return controlAreaRules;
    }

    public Map<String, Map<String, String>> errorMessagesByRule() {
        return errorMessagesByRule;
    }

    public Map<String, Set<String>> excludedBlocksByControlArea() {
        return excludedBlocksByControlArea;
    }

    public Map<String, Boolean> filterFlagsByControlArea() {
        return filterFlagsByControlArea;
    }

    public Map<String, String> genderMapping() {
        return genderMapping;
    }

    public Map<String, String> typeDulMapping() {
        return typeDulMapping;
    }

    public String compiledRulesVersion() {
        return compiledRulesVersion;
    }

    public String politicsVersion() {
        return politicsVersion;
    }

    public Map<String, Set<String>> referencedFieldsByRule() {
        return referencedFieldsByRule;
    }

    public Map<String, Set<String>> referencedFieldsByControlArea() {
        return referencedFieldsByControlArea;
    }

    public String controlAreaByDataset(String datasetCode) {
        return dataset2ControlArea.get(datasetCode);
    }

    public Map<String, Set<String>> fieldToRules(String controlArea) {
        return controlAreaRules.get(controlArea);
    }

    public Set<String> referencedFields(String controlArea) {
        Set<String> result = referencedFieldsByControlArea.get(controlArea);
        return result == null ? Set.of() : result;
    }

    public Set<String> referencedFieldsForRule(String ruleId) {
        Set<String> result = RuleIdLookup.resolve(referencedFieldsByRule, ruleId);
        return result == null ? Set.of() : result;
    }

    public Set<String> excludedBlocks(String controlArea) {
        Set<String> result = excludedBlocksByControlArea.get(controlArea);
        return result == null ? Set.of() : result;
    }

    public Boolean filterFlag(String controlArea) {
        return filterFlagsByControlArea.get(controlArea);
    }

    RulesCacheSnapshot withCompiledRules(
            Map<String, Rule> newCompiledRules,
            String newCompiledRulesVersion
    ) {
        return new RulesCacheSnapshot(
                newCompiledRules,
                dataset2ControlArea,
                controlAreaRules,
                errorMessagesByRule,
                excludedBlocksByControlArea,
                filterFlagsByControlArea,
                genderMapping,
                typeDulMapping,
                newCompiledRulesVersion,
                politicsVersion
        );
    }

    RulesCacheSnapshot withPolitics(
            Map<String, String> newDataset2ControlArea,
            Map<String, Map<String, Set<String>>> newControlAreaRules,
            Map<String, Map<String, String>> newErrorMessagesByRule,
            Map<String, Set<String>> newExcludedBlocksByControlArea,
            Map<String, Boolean> newFilterFlagsByControlArea,
            Map<String, String> newGenderMapping,
            Map<String, String> newTypeDulMapping,
            String newPoliticsVersion
    ) {
        // Набор скомпилированных правил не менялся, поэтому индекс ruleId ->
        // referencedFields переиспользуется. Перестраивается только дешёвое
        // объединение по новой конфигурации controlArea.
        return new RulesCacheSnapshot(
                compiledRules,
                newDataset2ControlArea,
                newControlAreaRules,
                newErrorMessagesByRule,
                newExcludedBlocksByControlArea,
                newFilterFlagsByControlArea,
                newGenderMapping,
                newTypeDulMapping,
                compiledRulesVersion,
                newPoliticsVersion,
                referencedFieldsByRule
        );
    }

    private static Map<String, Set<String>> buildReferencedFieldsByRule(
            Map<String, Rule> rules
    ) {
        if (rules == null || rules.isEmpty()) {
            return Map.of();
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();

        for (Map.Entry<String, Rule> entry : rules.entrySet()) {
            String ruleId = entry.getKey();
            Rule rule = entry.getValue();

            if (ruleId == null || ruleId.isBlank() || rule == null) {
                continue;
            }

            Set<String> rawFields;
            try {
                rawFields = rule.referencedFields();
            } catch (RuntimeException ex) {
                throw new IllegalStateException(
                        "Failed to read referencedFields from rule '" + ruleId + "'",
                        ex
                );
            }

            if (rawFields == null) {
                throw new IllegalStateException(
                        "Rule '" + ruleId + "' returned null from referencedFields()"
                );
            }

            LinkedHashSet<String> fields = new LinkedHashSet<>();
            for (String field : rawFields) {
                if (field == null || field.isBlank()) {
                    throw new IllegalStateException(
                            "Rule '" + ruleId + "' contains null/blank referenced field"
                    );
                }
                fields.add(field.trim());
            }

            result.put(ruleId, freezeSet(fields));
        }

        return Collections.unmodifiableMap(result);
    }

    private static Map<String, Set<String>> buildReferencedFieldsByControlArea(
            Map<String, Map<String, Set<String>>> rulesByControlArea,
            Map<String, Set<String>> fieldsByRule
    ) {
        if (rulesByControlArea == null || rulesByControlArea.isEmpty()) {
            return Map.of();
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, Set<String>>> areaEntry
                : rulesByControlArea.entrySet()) {
            String controlArea = areaEntry.getKey();
            if (controlArea == null || controlArea.isBlank()) {
                continue;
            }

            LinkedHashSet<String> referencedFields = new LinkedHashSet<>();
            Map<String, Set<String>> fieldToRules = areaEntry.getValue();

            if (fieldToRules != null) {
                for (Map.Entry<String, Set<String>> fieldEntry : fieldToRules.entrySet()) {
                    String anchorField = fieldEntry.getKey();
                    Set<String> ruleIds = fieldEntry.getValue();
                    if (ruleIds == null || ruleIds.isEmpty()) {
                        continue;
                    }

                    for (String ruleId : ruleIds) {
                        Set<String> ruleFields = RuleIdLookup.resolve(fieldsByRule, ruleId);

                        if (ruleFields == null || ruleFields.isEmpty()) {
                            // Переходный fallback для старого байткода, который
                            // использует default referencedFields(). Он сохраняет
                            // прежнюю защиту хотя бы для якорного атрибута.
                            addField(referencedFields, anchorField);
                        } else {
                            referencedFields.addAll(ruleFields);
                        }
                    }
                }
            }

            result.put(controlArea, freezeSet(referencedFields));
        }

        return Collections.unmodifiableMap(result);
    }

    private static void addField(Set<String> target, String field) {
        if (field != null && !field.isBlank()) {
            target.add(field.trim());
        }
    }

    private static Set<String> freezeSet(Set<String> source) {
        if (source == null || source.isEmpty()) {
            return Set.of();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<>(source));
    }

    private static <K, V> Map<K, V> safeMap(Map<K, V> value) {
        return value == null ? Map.of() : value;
    }
}
