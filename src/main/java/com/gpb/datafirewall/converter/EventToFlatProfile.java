package com.gpb.datafirewall.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.dto.FlatProfileDto;

import java.util.Iterator;
import java.util.Map;

/**
 * Преобразует входящее событие в «плоский» профиль на основе mapping-описаний в JSON.
 *
 * <p>Класс читает структуру события и строит новый JSON-объект, в который
 * проецируются значения полей согласно правилам вида:</p>
 *
 * <pre>
 *     "mapping.someField": "targetName"
 * </pre>
 *
 * <p>То есть значение поля {@code someField} будет записано в результирующий профиль
 * под именем {@code targetName}.</p>
 *
 * <p>Поддерживает обработку разделов {@code baseInfo}, {@code documents} и
 * специальную логику выбора основной карточки клиента ({@code primary=true}).</p>
 *
 * <p>Невалидные mapping-значения (null, "", "none") игнорируются.</p>
 */

public final class EventToFlatProfile {

    private static final String DATA = "data";
    private static final String MAPPING_PREFIX = "mapping.";
    private static final int MAPPING_PREFIX_LEN = MAPPING_PREFIX.length();

    private final ObjectMapper mapper;

    public EventToFlatProfile(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode convert(JsonNode eventJson) {
        ObjectNode out = mapper.createObjectNode();

        if (eventJson == null || eventJson.isNull()) {
            return out;
        }

        JsonNode data = eventJson.get(DATA);
        if (data == null || data.isNull() || !data.isObject()) {
            return out;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String blockName = entry.getKey();
            JsonNode section = entry.getValue();

            if (section == null || !section.isObject()) {
                continue;
            }

            projectSection(section, out);

            // специальная логика для документов (clientIdCard)
            // обрабатываем все элементы массива, а не только primary
            if ("documents".equals(blockName)) {
                projectArray(section.get("clientIdCard"), out);
            }
        }

        return out;
    }

    public FlatProfileDto convertToProfile(JsonNode eventJson) {
        return new FlatProfileDto(convert(eventJson));
    }

    private void projectSection(JsonNode section, ObjectNode out) {
        if (section == null || !section.isObject()) {
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = section.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();

            String key = e.getKey();
            if (key == null || !key.startsWith(MAPPING_PREFIX)) {
                continue;
            }

            JsonNode mappingNode = e.getValue();
            if (!isValidMappingNode(mappingNode)) {
                continue;
            }

            String sourceFieldName = key.substring(MAPPING_PREFIX_LEN).trim();
            if (sourceFieldName.isEmpty()) {
                continue;
            }

            String targetKey = mappingNode.asText().trim();

            if (out.get(targetKey) != null) {
                continue;
            }

            JsonNode valueNode = section.get(sourceFieldName);
            out.set(targetKey, normalizeValue(valueNode));
        }
    }

    private boolean isValidMappingNode(JsonNode mappingNode) {
        if (mappingNode == null || mappingNode.isNull() || !mappingNode.isValueNode()) {
            return false;
        }

        String value = mappingNode.asText();
        if (value == null) {
            return false;
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return false;
        }

        return !isNoneOrNullLiteral(trimmed);
    }

    private boolean isNoneOrNullLiteral(String value) {
        return value.length() == 4 && value.equalsIgnoreCase("none")
                || value.length() == 4 && value.equalsIgnoreCase("null");
    }

    private JsonNode normalizeValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isMissingNode() || valueNode.isNull()) {
            return mapper.nullNode();
        }

        // Сохраняем объекты, массивы, числа, boolean как есть.
        // Пустую строку превращаем в JSON null.
        if (valueNode.isTextual() && (valueNode.asText().isEmpty() || "none".equalsIgnoreCase(valueNode.asText()))) {
            return mapper.nullNode();
        }

        return valueNode;
    }

    private void projectArray(JsonNode arrayNode, ObjectNode out) {
        if (arrayNode == null || !arrayNode.isArray() || arrayNode.isEmpty()) {
            return;
        }

        for (JsonNode item : arrayNode) {
            if (item == null || item.isNull() || !item.isObject()) {
                continue;
            }

            projectSection(item, out);
        }
    }
}
