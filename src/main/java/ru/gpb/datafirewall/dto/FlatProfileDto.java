package ru.gpb.datafirewall.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FlatProfileDto {

    private final ObjectNode data;

    public FlatProfileDto(ObjectNode data) {
        this.data = data;
    }

    /** Исходный JSON-профиль (read-only view). */
    public ObjectNode json() {
        return data;
    }

    public JsonNode get(String key) {
        return key == null ? null : data.get(key);
    }

    public String getText(String key) {
        JsonNode n = get(key);
        return (n == null || n.isNull()) ? null : n.asText();
    }

    /**
     * Представление профиля как Map<String, String> для Rule.apply(...).
     *
     * <ul>
     *   <li>null / JSON null → null</li>
     *   <li>scalar → asText()</li>
     *   <li>object/array → JSON string</li>
     * </ul>
     *
     * Порядок ключей детерминирован.
     */
    public Map<String, String> asStringMap() {
        Map<String, String> out = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = data.fields();

        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            JsonNode v = e.getValue();

            if (key == null || key.isBlank()) continue;

            String str;
            if (v == null || v.isNull()) {
                str = null;
            } else if (v.isValueNode()) {
                str = v.asText();
            } else {
                str = v.toString();
            }

            out.put(key, str);
        }
        return out;
    }
}
