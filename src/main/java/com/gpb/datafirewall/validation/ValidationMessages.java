package com.gpb.datafirewall.validation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Сообщения сработавших правил, разделённые по severity.
 * Ключ карты: logicalField либо instanceKey + "::" + logicalField.
 */
public record ValidationMessages(
        Map<String, List<String>> errorsByField,
        Map<String, List<String>> warningsByField,
        Set<String> scopedInstanceKeys
) {
    public ValidationMessages {
        errorsByField = errorsByField == null ? Map.of() : errorsByField;
        warningsByField = warningsByField == null ? Map.of() : warningsByField;
        scopedInstanceKeys = scopedInstanceKeys == null ? Set.of() : scopedInstanceKeys;
    }
}
