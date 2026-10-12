package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class MappingNormalizer {

    private static final String GENDER_FIELD = "ОСНОВНЫЕ СВЕДЕНИЯ.Пол";
    private static final String TYPE_DUL_FIELD_PREFIX = "ДУЛ.";
    private static final String TYPE_DUL_FIELD_SUFFIX = ".Тип ДУЛ";

    private final ObjectMapper mapper;

    public MappingNormalizer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Универсальная нормализация:
     * - проходит по всему event.data рекурсивно
     * - для каждого объекта берёт mapping.* = logicalName
     * - находит рядом поле без "mapping." и кладёт logicalName -> value
     * - сразу подставляет справочные значения пола и типа ДУЛ
     * - при filterFlag=true дополняет результат отсутствующими атрибутами
     *   текущей контрольной области со значением null
     *
     * Если logicalName повторяется (например массив документов) — склеиваем через ';'
     *
     * ВАЖНО:
     * значение mapping.* = "none" означает, что поле не нужно добавлять.
     * Строка "none" в самом физическом значении остаётся обычным значением.
     */
    public Map<String, String> normalize(JsonNode event) {
        return normalize(event, Map.of(), Map.of());
    }

    public Map<String, String> normalize(
            JsonNode event,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping
    ) {
        return normalize(
                event,
                genderMapping,
                typeDulMapping,
                Map.of(),
                false
        );
    }

    public Map<String, String> normalize(
            JsonNode event,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping,
            Map<String, ?> expectedFields,
            Boolean filterFlag
    ) {
        Map<String, String> out = new LinkedHashMap<>();

        JsonNode data = event == null ? null : event.get("data");
        if (data == null || data.isNull()) {
            completeMissingFields(out, expectedFields, filterFlag);
            return out;
        }

        Map<String, String> genders = genderMapping == null ? Map.of() : genderMapping;
        Map<String, String> documentTypes = typeDulMapping == null ? Map.of() : typeDulMapping;

        walk(data, out, genders, documentTypes);
        completeMissingFields(out, expectedFields, filterFlag);
        return out;
    }

    /**
     * При включённом filterFlag дополняет результат нормализации всеми
     * атрибутами текущей контрольной области. Уже нормализованные значения
     * не перезаписываются, отсутствующие атрибуты получают значение null.
     *
     * Метод изменяет и возвращает переданную LinkedHashMap, созданную этапом
     * нормализации. Благодаря этому для каждого сообщения нет лишнего
     * копирования всей карты.
     */
    Map<String, String> completeMissingFields(
            Map<String, String> normalizedMap,
            Map<String, ?> expectedFields,
            Boolean filterFlag
    ) {
        Map<String, String> result = normalizedMap == null
                ? new LinkedHashMap<>()
                : normalizedMap;

        if (!Boolean.TRUE.equals(filterFlag)
                || expectedFields == null
                || expectedFields.isEmpty()) {
            return result;
        }

        completeMissingFieldsAndSelectPresent(result, expectedFields);
        return result;
    }

    /**
     * Одним проходом дополняет карту отсутствующими ключами и возвращает
     * только те записи fieldToRules, чьи атрибуты реально присутствовали до
     * дополнения. Это сохраняет семантику filterFlag: правила отсутствующего
     * атрибута пропускаются, но правило присутствующего атрибута видит все
     * свои зависимости как ключи со значением null.
     */
    <V> Map<String, V> completeMissingFieldsAndSelectPresent(
            Map<String, String> normalizedMap,
            Map<String, V> expectedFields
    ) {
        Map<String, V> presentFields = new LinkedHashMap<>();
        if (normalizedMap == null || expectedFields == null || expectedFields.isEmpty()) {
            return presentFields;
        }

        // Нужен только для редкого случая, когда кэш одновременно содержит
        // варианты имени с точкой и запятой. Синтетически добавленный ключ не
        // должен считаться физически присутствовавшим для следующей записи.
        Set<String> syntheticFields = null;

        for (Map.Entry<String, V> entry : expectedFields.entrySet()) {
            String logicalField = entry.getKey();
            if (logicalField == null || logicalField.isBlank()) {
                continue;
            }

            if (normalizedMap.containsKey(logicalField)
                    && (syntheticFields == null || !syntheticFields.contains(logicalField))) {
                presentFields.put(logicalField, entry.getValue());
                continue;
            }

            // Сохраняем поддержку старого варианта имени, где вместо точки
            // в mapping использовалась запятая.
            String alternativeField = logicalField.replace('.', ',');
            if (!alternativeField.equals(logicalField)
                    && normalizedMap.containsKey(alternativeField)
                    && (syntheticFields == null || !syntheticFields.contains(alternativeField))) {
                normalizedMap.put(logicalField, normalizedMap.get(alternativeField));
                presentFields.put(logicalField, entry.getValue());
            } else {
                normalizedMap.put(logicalField, null);
                if (syntheticFields == null) {
                    syntheticFields = new HashSet<>();
                }
                syntheticFields.add(logicalField);
            }
        }

        return presentFields;
    }

    private void walk(
            JsonNode node,
            Map<String, String> out,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping
    ) {
        if (node == null || node.isNull()) {
            return;
        }

        if (node.isObject()) {
            extractMappings(node, out, genderMapping, typeDulMapping);

            Iterator<Map.Entry<String, JsonNode>> it = node.fields();

            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();

                JsonNode v = e.getValue();

                if (v == null || v.isNull()) {
                    continue;
                }

                if (v.isTextual() && v.asText().isEmpty()) {
                    v = mapper.nullNode();
                }

                if (v.isObject() || v.isArray()) {
                    walk(v, out, genderMapping, typeDulMapping);
                }
            }

        } else if (node.isArray()) {
            for (JsonNode el : node) {
                walk(el, out, genderMapping, typeDulMapping);
            }
        }
    }

    private void extractMappings(
            JsonNode obj,
            Map<String, String> out,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping
    ) {
        Iterator<Map.Entry<String, JsonNode>> it = obj.fields();

        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();

            String k = e.getKey();
            JsonNode v = e.getValue();

            if (k == null || !k.startsWith("mapping.")) {
                continue;
            }

            if (v == null || v.isNull()) {
                continue;
            }

            String rawField =
                    k.substring("mapping.".length());

            String logical = v.asText(null);

            if (logical == null
                    || logical.isBlank()
                    || "none".equalsIgnoreCase(logical.trim())) {
                continue;
            }

            logical = logical.trim();

            // ВАЖНО:
            // mapping есть, но самого физического атрибута нет.
            // В normalizedMap такое поле добавлять нельзя.
            if (!obj.has(rawField)) {
                continue;
            }

            JsonNode valueNode =
                    obj.get(rawField);

            String value =
                    toFlatString(valueNode);

            value = mapReferenceValue(
                    logical,
                    value,
                    genderMapping,
                    typeDulMapping
            );

            String prev =
                    out.get(logical);

            if (prev == null || prev.isBlank()) {
                out.put(logical, value);
            } else {
                out.put(
                        logical,
                        prev + ";" + value
                );
            }
        }
    }

    private String mapReferenceValue(
            String logicalField,
            String currentValue,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping
    ) {
        if (currentValue == null || currentValue.isBlank()) {
            return currentValue;
        }

        Map<String, String> valueMapping;
        if (GENDER_FIELD.equals(logicalField)) {
            valueMapping = genderMapping;
        } else if (isTypeDulField(logicalField)) {
            valueMapping = typeDulMapping;
        } else {
            return currentValue;
        }

        String lookupValue = currentValue.trim();
        String mappedValue = valueMapping.get(lookupValue);

        if (mappedValue == null) {
            mappedValue = valueMapping.get(lookupValue.toUpperCase(Locale.ROOT));
        }

        return mappedValue == null ? currentValue : mappedValue;
    }

    private static boolean isTypeDulField(String logicalField) {
        return logicalField.length() >= TYPE_DUL_FIELD_PREFIX.length() + TYPE_DUL_FIELD_SUFFIX.length()
                && logicalField.startsWith(TYPE_DUL_FIELD_PREFIX)
                && logicalField.endsWith(TYPE_DUL_FIELD_SUFFIX);
    }

    private String toFlatString(JsonNode v) {
        if (v == null || v.isNull()) {
            return null;
        }

        if (v.isValueNode()) {
            return v.asText();
        }

        try {
            return mapper.writeValueAsString(v);
        } catch (Exception e) {
            return v.toString();
        }
    }
}
