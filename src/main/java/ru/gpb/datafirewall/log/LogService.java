package ru.gpb.datafirewall.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Set;

public class LogService {


    private static final ObjectMapper OM = new ObjectMapper();

    // что маскируем (расширяй список)
    private static final Set<String> SENSITIVE_KEYS = Set.of(
            "birthdate", "clientSnils", "snils", "inn",
            "number", "series", "departmentCode"
    );

    private LogService() {
    }

    public static String maskJson(String json) {
        if (json == null || json.isBlank()) return json;
        try {
            JsonNode root = OM.readTree(json);
            maskNode(root);
            return OM.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            // если не парсится — не падаем
            return json;
        }
    }

    private static void maskNode(JsonNode node) {
        if (node == null) return;
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            obj.fieldNames().forEachRemaining(fn -> {
                JsonNode child = obj.get(fn);
                if (SENSITIVE_KEYS.contains(fn)) {
                    obj.put(fn, "***");
                } else {
                    maskNode(child);
                }
            });
        } else if (node.isArray()) {
            for (JsonNode child : node) maskNode(child);
        }
    }
}

