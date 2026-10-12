package com.gpb.datafirewall.cache;

/**
 * Versioned cache для gender_mapping: исходное значение пола -> код.
 *
 * <p>Структура: Map<String, String></p>
 */
public final class GenderMappingCache
        extends AtomicSnapshotCache<String, String> {
}
