package com.gpb.datafirewall.converter;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Универсальный класс - конвертер для кэша (приведение типов)
 */
public final class CachePayloadConverter {

    private final ObjectMapper mapper;

    private final JavaType stringType;
    private final JavaType booleanType;
    private final JavaType setOfStringType;
    private final JavaType mapStringStringType;
    private final JavaType mapStringSetStringType;

    public CachePayloadConverter(ObjectMapper mapper) {
        this.mapper = mapper;

        var tf = mapper.getTypeFactory();

        this.stringType = tf.constructType(String.class);
        this.booleanType = tf.constructType(Boolean.class);

        this.setOfStringType = tf.constructCollectionType(Set.class, String.class);

        this.mapStringStringType = tf.constructMapType(
                LinkedHashMap.class,
                String.class,
                String.class
        );

        this.mapStringSetStringType = tf.constructMapType(
                LinkedHashMap.class,
                tf.constructType(String.class),
                setOfStringType
        );
    }

    public Map<String, String> toStringMap(
            Map<String, Object> payload,
            String cacheName
    ) {
        return convertPayload(payload, cacheName, stringType);
    }

    public Map<String, Boolean> toBooleanMap(
            Map<String, Object> payload,
            String cacheName
    ) {
        return convertPayload(payload, cacheName, booleanType);
    }

    public Map<String, Set<String>> toSetMap(
            Map<String, Object> payload,
            String cacheName
    ) {
        return convertPayload(payload, cacheName, setOfStringType);
    }

    public Map<String, Map<String, String>> toNestedStringMap(
            Map<String, Object> payload,
            String cacheName
    ) {
        return convertPayload(payload, cacheName, mapStringStringType);
    }

    public Map<String, Map<String, Set<String>>> toNestedRulesMap(
            Map<String, Object> payload,
            String cacheName
    ) {
        return convertPayload(payload, cacheName, mapStringSetStringType);
    }

    private <V> Map<String, V> convertPayload(
            Map<String, Object> payload,
            String cacheName,
            JavaType valueType
    ) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        Map<String, V> result = new LinkedHashMap<>(capacityFor(payload.size()));

        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            String key = entry.getKey();
            Object rawValue = entry.getValue();

            if (key == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }
            if (rawValue == null) {
                throw new IllegalStateException(
                        "Null value for key '" + key + "' in cache " + cacheName
                );
            }

            V value = mapper.convertValue(rawValue, valueType);
            result.put(key, value);
        }

        return result;
    }

    private static int capacityFor(int expectedSize) {
        if (expectedSize <= 0) {
            return 16;
        }
        return Math.max(16, (int) (expectedSize / 0.75f) + 1);
    }
}
