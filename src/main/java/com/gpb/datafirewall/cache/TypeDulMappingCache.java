package com.gpb.datafirewall.cache;

/**
 * Versioned cache для type_dul_mapping: исходный тип документа -> код.
 *
 * <p>Структура: Map<String, String></p>
 */
public final class TypeDulMappingCache
        extends AtomicSnapshotCache<String, String> {
}
