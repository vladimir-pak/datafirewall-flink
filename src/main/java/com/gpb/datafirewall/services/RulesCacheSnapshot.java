package com.gpb.datafirewall.services;

import com.gpb.datafirewall.model.Rule;

import java.util.Map;
import java.util.Set;

/**
 * Единый immutable snapshot всех кэшей, используемых при обработке сообщения.
 *
 * <p>Каждый входящий event получает одну ссылку на этот объект и до конца
 * обработки работает только с ней. Публикация новой версии выполняется одной
 * атомарной заменой ссылки в {@link RulesCacheRuntime}.</p>
 */
public record RulesCacheSnapshot(
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

    public RulesCacheSnapshot {
        compiledRules = safeMap(compiledRules);
        dataset2ControlArea = safeMap(dataset2ControlArea);
        controlAreaRules = safeMap(controlAreaRules);
        errorMessagesByRule = safeMap(errorMessagesByRule);
        excludedBlocksByControlArea = safeMap(excludedBlocksByControlArea);
        filterFlagsByControlArea = safeMap(filterFlagsByControlArea);
        genderMapping = safeMap(genderMapping);
        typeDulMapping = safeMap(typeDulMapping);
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

    public String controlAreaByDataset(String datasetCode) {
        return dataset2ControlArea.get(datasetCode);
    }

    public Map<String, Set<String>> fieldToRules(String controlArea) {
        return controlAreaRules.get(controlArea);
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
                newPoliticsVersion
        );
    }

    private static <K, V> Map<K, V> safeMap(Map<K, V> value) {
        return value == null ? Map.of() : value;
    }
}
