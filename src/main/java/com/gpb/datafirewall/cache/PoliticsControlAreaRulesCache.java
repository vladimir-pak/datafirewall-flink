package com.gpb.datafirewall.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Versioned cache для politics_control_area_rules.
 *
 * <p>Структура:</p>
 * <pre>
 * Map<String, Map<String, Set<String>>>
 *
 * где:
 *   key   = controlArea
 *   value = Map<logicalField, Set<ruleNames>>
 * </pre>
 */
public final class PoliticsControlAreaRulesCache
        extends AtomicSnapshotCache<String, Map<String, Set<String>>> {

    @Override
    protected Map<String, Map<String, Set<String>>> freeze(
            Map<String, Map<String, Set<String>>> source) {

        if (source == null || source.isEmpty()) {
            return Map.of();
        }

        Map<String, Map<String, Set<String>>> outerCopy = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, Set<String>>> outerEntry : source.entrySet()) {
            String controlArea = outerEntry.getKey();
            if (controlArea == null) {
                continue;
            }

            Map<String, Set<String>> innerMap = outerEntry.getValue();
            Map<String, Set<String>> innerCopy = new LinkedHashMap<>();

            if (innerMap != null) {
                for (Map.Entry<String, Set<String>> innerEntry : innerMap.entrySet()) {
                    String field = innerEntry.getKey();
                    if (field == null) {
                        continue;
                    }

                    Set<String> rules = innerEntry.getValue();

                    Set<String> frozenSet = (rules == null || rules.isEmpty())
                            ? Set.of()
                            : Collections.unmodifiableSet(new HashSet<>(rules));

                    innerCopy.put(field, frozenSet);
                }
            }

            outerCopy.put(
                    controlArea,
                    Collections.unmodifiableMap(innerCopy)
            );
        }

        return Collections.unmodifiableMap(outerCopy);
    }
}