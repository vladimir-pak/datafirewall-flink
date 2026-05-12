package ru.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class MappingNormalizer {

    private final ObjectMapper mapper;

    public MappingNormalizer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Универсальная нормализация:
     * - проходит по всему event.data рекурсивно
     * - для каждого объекта берёт mapping.* = logicalName
     * - находит рядом поле без "mapping." и кладёт logicalName -> value
     *
     * Если logicalName повторяется (например массив документов) — склеиваем через ';'
     */
    public Map<String, String> normalize(JsonNode event) {
        Map<String, String> out = new LinkedHashMap<>();

        JsonNode data = event == null ? null : event.get("data");
        if (data == null || data.isNull()) return out;

        walk(data, out);
        return out;
    }

    private void walk(JsonNode node, Map<String, String> out) {
        if (node == null || node.isNull()) return;

        if (node.isObject()) {
            extractMappings(node, out);

            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                JsonNode v = e.getValue();
                if (v == null || v.isNull()) continue;
                if (v.isObject() || v.isArray()) {
                    walk(v, out);
                }
            }

        } else if (node.isArray()) {
            for (JsonNode el : node) {
                walk(el, out);
            }
        }
    }

    private void extractMappings(JsonNode obj, Map<String, String> out) {
        Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String k = e.getKey();
            JsonNode v = e.getValue();

            if (k == null || !k.startsWith("mapping.")) continue;
            if (v == null || v.isNull()) continue;

            String rawField = k.substring("mapping.".length());
            String logical = v.asText(null);

            if (logical == null || logical.isBlank() || "none".equalsIgnoreCase(logical)) continue;

            JsonNode valueNode = obj.get(rawField);
            if (valueNode == null || valueNode.isNull()) continue;

            String value = toFlatString(valueNode);

            String prev = out.get(logical);
            if (prev == null || prev.isBlank()) {
                out.put(logical, value);
            } else {
                out.put(logical, prev + ";" + value);
            }
        }
    }

    private String toFlatString(JsonNode v) {
        if (v == null || v.isNull()) return null;
        if (v.isValueNode()) return v.asText();
        try {
            return mapper.writeValueAsString(v);
        } catch (Exception e) {
            return v.toString();
        }
    }
}