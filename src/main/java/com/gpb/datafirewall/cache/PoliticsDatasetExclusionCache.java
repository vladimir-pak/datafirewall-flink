package com.gpb.datafirewall.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Versioned cache для politics_dataset_exclusion.
 *
 * <p>Структура: Map<String, Set<String>></p>
 */
public final class PoliticsDatasetExclusionCache
        extends AtomicSnapshotCache<String, Set<String>> {

    @Override
    protected Map<String, Set<String>> freeze(Map<String, Set<String>> source) {
        Map<String, Set<String>> copy = new HashMap<>();

        for (Map.Entry<String, Set<String>> entry : source.entrySet()) {
            Set<String> frozenSet = entry.getValue() == null
                    ? Set.of()
                    : Collections.unmodifiableSet(new HashSet<>(entry.getValue()));

            copy.put(entry.getKey(), frozenSet);
        }

        return Collections.unmodifiableMap(copy);
    }
}