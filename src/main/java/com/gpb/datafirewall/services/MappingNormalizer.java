package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class MappingNormalizer {

    private static final Logger log = LoggerFactory.getLogger(MappingNormalizer.class);

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
     *
     * Если logicalName повторяется, сохраняем первое физически присутствующее
     * значение, включая null и пустую строку. Повторы не склеиваются.
     * Для каждого повторяющегося logicalName выводится один WARN на вызов normalize.
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
        NormalizationContext context = new NormalizationContext();

        JsonNode data = event == null ? null : event.get("data");
        if (data == null || data.isNull()) {
            return context.values;
        }

        Map<String, String> genders = genderMapping == null ? Map.of() : genderMapping;
        Map<String, String> documentTypes = typeDulMapping == null ? Map.of() : typeDulMapping;

        walk(data, context, genders, documentTypes);
        return context.values;
    }

    /**
     * Совместимый overload. expectedFields и filterFlag больше не изменяют
     * нормализованную карту: в ней остаются только атрибуты входного JSON.
     */
    public Map<String, String> normalize(
            JsonNode event,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping,
            Map<String, ?> expectedFields,
            Boolean filterFlag
    ) {
        return normalize(event, genderMapping, typeDulMapping);
    }

    /**
     * Возвращает только привязки правил для атрибутов, которые присутствовали
     * во входных данных. Карта normalizedMap может быть дополнена только
     * каноническим вариантом имени для legacy-ключа с запятой; синтетические
     * null здесь никогда не создаются.
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

    private void walk(
            JsonNode node,
            NormalizationContext context,
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
                            context,
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
            // затем обходим вложенные узлы. Значение родителя имеет приоритет
            // при повторении logicalName во вложенном объекте.
            if (nestedNodes != null) {
                for (JsonNode nestedNode : nestedNodes) {
                    walk(nestedNode, context, genderMapping, typeDulMapping);
                }
            }

        } else if (node.isArray()) {
            for (JsonNode el : node) {
                walk(el, context, genderMapping, typeDulMapping);
            }
        }
    }

    private void extractMapping(
            JsonNode obj,
            String mappingKey,
            JsonNode mappingNode,
            NormalizationContext context,
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

        // containsKey сохраняет первое значение даже при null или "".
        // Дубликат не требует преобразования значения или lookup в справочнике.
        if (context.values.containsKey(logical)) {
            context.warnDuplicate(logical);
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

        context.values.put(logical, value);
    }

    /** Состояние одного вызова: не разделяется между сообщениями и потоками. */
    private static final class NormalizationContext {
        private final Map<String, String> values = new LinkedHashMap<>();
        private Set<String> warnedFields;

        private void warnDuplicate(String logicalField) {
            if (!log.isWarnEnabled()) return;
            // Дополнительный Set создаётся только при первом дубликате.
            if (warnedFields == null) warnedFields = new HashSet<>();
            if (warnedFields.add(logicalField)) {
                log.warn("[NORMALIZE] duplicate mapping: field={} policy=FIRST", logicalField);
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

        if (v.isArray()) {
            // Пустой массив [] преобразуем в null
            if (v.size() == 0) {
                return null;
            }

            // Массив с одним скалярным элементом
            if (v.size() == 1) {
                JsonNode first = v.get(0);

                if (first.isValueNode()) {
                    return toFlatString(first);
                }
            }
        }

        try {
            return mapper.writeValueAsString(v);
        } catch (Exception e) {
            return v.toString();
        }
    }
}
