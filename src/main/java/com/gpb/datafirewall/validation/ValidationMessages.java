package com.gpb.datafirewall.validation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

/**
 * Сообщения по типам нарушения и отдельные результаты excluded-блоков.
 * Адресация excluded-блоков не зависит от dataset_code (он может повторяться).
 */
public record ValidationMessages(
        Map<String, List<String>> errorsByField,
        Map<String, List<String>> warningsByField,
        Set<String> scopedInstanceKeys,
        Map<String, Map<String, Map<String, String>>> excludedBlockStatuses
) {
    private static final String EXCLUDED_SCOPE_PREFIX = "$excludedBlock::";

    // Старый конструктор сохраняется.
    public ValidationMessages(
            Map<String, List<String>> errorsByField,
            Map<String, List<String>> warningsByField,
            Set<String> scopedInstanceKeys
    ) {
        this(errorsByField, warningsByField, scopedInstanceKeys, Map.of());
    }

    public ValidationMessages {
        errorsByField = errorsByField == null ? Map.of() : errorsByField;
        warningsByField = warningsByField == null ? Map.of() : warningsByField;
        scopedInstanceKeys = scopedInstanceKeys == null ? Set.of() : scopedInstanceKeys;
        excludedBlockStatuses = excludedBlockStatuses == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(excludedBlockStatuses));
    }

    public static String excludedScopeKey(String blockName) {
        return EXCLUDED_SCOPE_PREFIX + blockName;
    }

    public static boolean isExcludedScopeKey(String key) {
        return key != null && key.startsWith(EXCLUDED_SCOPE_PREFIX);
    }
}
