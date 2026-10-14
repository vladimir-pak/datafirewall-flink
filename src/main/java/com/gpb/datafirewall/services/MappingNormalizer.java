package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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
     * - совместимый overload может дополнить переданным набором expectedFields
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
     * Совместимый метод для старых вызовов: при выключенном filterFlag
     * дополняет результат переданным набором expectedFields. Основной pipeline
     * использует раздельные selectPresentFields и completeMissingFields.
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

        if (!Boolean.FALSE.equals(filterFlag)
                || expectedFields == null
                || expectedFields.isEmpty()) {
            return result;
        }

        completeMissingFieldsAndSelectPresent(result, expectedFields);
        return result;
    }

    /**
     * Совместимый комбинированный метод: сначала выбирает записи fieldToRules,
     * чьи атрибуты реально присутствовали, затем дополняет отсутствующие ключи.
     */
    <V> Map<String, V> completeMissingFieldsAndSelectPresent(
            Map<String, String> normalizedMap,
            Map<String, V> expectedFields
    ) {
        Map<String, V> presentFields = selectPresentFields(normalizedMap, expectedFields);
        completeMissingFields(normalizedMap, expectedFields == null ? Set.of() : expectedFields.keySet());
        return presentFields;
    }

    /**
     * Возвращает только привязки правил для атрибутов, которые присутствовали
     * во входных данных до null-дополнения. Карта normalizedMap может быть
     * дополнена только каноническим вариантом имени для legacy-ключа с
     * запятой; синтетические null здесь никогда не создаются.
     */
    <V> Map<String, V> selectPresentFields(
            Map<String, String> normalizedMap,
            Map<String, V> fieldToRules
    ) {
        Map<String, V> presentFields = new LinkedHashMap<>();
        if (normalizedMap == null || fieldToRules == null || fieldToRules.isEmpty()) {
            return presentFields;
        }

        for (Map.Entry<String, V> entry : fieldToRules.entrySet()) {
            String logicalField = entry.getKey();
            if (logicalField == null || logicalField.isBlank()) {
                continue;
            }

            if (normalizedMap.containsKey(logicalField)) {
                presentFields.put(logicalField, entry.getValue());
                continue;
            }

            String alternativeField = logicalField.replace('.', ',');
            if (!alternativeField.equals(logicalField)
                    && normalizedMap.containsKey(alternativeField)) {
                normalizedMap.put(logicalField, normalizedMap.get(alternativeField));
                presentFields.put(logicalField, entry.getValue());
            }
        }

        return presentFields;
    }

    /**
     * Добавляет отсутствующие referencedFields в общую карту Rule.apply.
     * Уже существующие значения, включая физический null, не изменяются.
     */
    Map<String, String> completeMissingFields(
            Map<String, String> normalizedMap,
            Iterable<String> referencedFields
    ) {
        Map<String, String> result = normalizedMap == null
                ? new LinkedHashMap<>()
                : normalizedMap;

        if (referencedFields == null) {
            return result;
        }

        for (String logicalField : referencedFields) {
            completeMissingField(result, logicalField);
        }

        return result;
    }

    /**
     * Локальный overlay получает null только для полей, которые одновременно
     * принадлежат текущему блоку и используются его правилами. Межблочные
     * зависимости остаются в общей карте и не перекрываются локальным null.
     */
    Map<String, String> completeMissingLocalFields(
            Map<String, String> normalizedMap,
            Set<String> referencedFields,
            Set<String> localFields
    ) {
        Map<String, String> result = normalizedMap == null
                ? new LinkedHashMap<>()
                : normalizedMap;

        if (referencedFields == null || referencedFields.isEmpty()
                || localFields == null || localFields.isEmpty()) {
            return result;
        }

        // Обычно localFields существенно меньше полного набора зависимостей
        // controlArea, особенно для межблочных правил.
        for (String logicalField : localFields) {
            if (referencedFields.contains(logicalField)) {
                completeMissingField(result, logicalField);
            }
        }

        return result;
    }

    private void completeMissingField(
            Map<String, String> normalizedMap,
            String logicalField
    ) {
        if (logicalField == null || logicalField.isBlank()
                || normalizedMap.containsKey(logicalField)) {
            return;
        }

        String alternativeField = logicalField.replace('.', ',');
        if (!alternativeField.equals(logicalField)
                && normalizedMap.containsKey(alternativeField)) {
            normalizedMap.put(logicalField, normalizedMap.get(alternativeField));
        } else {
            normalizedMap.put(logicalField, null);
        }
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
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            List<JsonNode> nestedNodes = null;

            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                JsonNode v = e.getValue();

                if (key != null && key.startsWith("mapping.")) {
                    extractMapping(
                            node,
                            key,
                            v,
                            out,
                            genderMapping,
                            typeDulMapping
                    );
                }

                if (v != null && !v.isNull() && (v.isObject() || v.isArray())) {
                    if (nestedNodes == null) {
                        nestedNodes = new ArrayList<>();
                    }
                    nestedNodes.add(v);
                }
            }

            // Как и раньше, сначала извлекаем mapping.* текущего объекта,
            // затем обходим вложенные узлы. Это сохраняет порядок склейки
            // повторяющихся logicalName, но не требует второго прохода fields().
            if (nestedNodes != null) {
                for (JsonNode nestedNode : nestedNodes) {
                    walk(nestedNode, out, genderMapping, typeDulMapping);
                }
            }

        } else if (node.isArray()) {
            for (JsonNode el : node) {
                walk(el, out, genderMapping, typeDulMapping);
            }
        }
    }

    private void extractMapping(
            JsonNode obj,
            String mappingKey,
            JsonNode mappingNode,
            Map<String, String> out,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping
    ) {
        if (mappingNode == null || mappingNode.isNull()) {
            return;
        }

        String rawField =
                mappingKey.substring("mapping.".length());

        String logical = mappingNode.asText(null);

        if (logical == null
                || logical.isBlank()
                || "none".equalsIgnoreCase(logical.trim())) {
            return;
        }

        logical = logical.trim();

        // mapping есть, но самого физического атрибута нет.
        // В normalizedMap такое поле добавлять нельзя.
        JsonNode valueNode = obj.get(rawField);
        if (valueNode == null) {
            return;
        }

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

        // Массив с одним скалярным элементом
        if (v.isArray() && v.size() == 1) {
            JsonNode first = v.get(0);

            if (first.isValueNode()) {
                return toFlatString(first);
            }
        }

        try {
            return mapper.writeValueAsString(v);
        } catch (Exception e) {
            return v.toString();
        }
    }
}
