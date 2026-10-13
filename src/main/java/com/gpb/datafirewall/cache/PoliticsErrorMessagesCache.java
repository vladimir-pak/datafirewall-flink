package com.gpb.datafirewall.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Versioned cache для politics_error_messages.
 *
 * <p>Структура: Map<String, String></p>
 */
public final class PoliticsErrorMessagesCache
        extends AtomicSnapshotCache<String, Map<String, String>> {

    @Override
    protected Map<String, Map<String, String>> freeze(
            Map<String, Map<String, String>> source
    ) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }

        Map<String, Map<String, String>> outerCopy = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
            Map<String, String> errorInfo = entry.getValue();
            Map<String, String> frozenErrorInfo = errorInfo == null || errorInfo.isEmpty()
                    ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(errorInfo));

            outerCopy.put(entry.getKey(), frozenErrorInfo);
        }

        return Collections.unmodifiableMap(outerCopy);
    }
}
