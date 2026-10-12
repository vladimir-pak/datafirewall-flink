package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class MappingNormalizerMappingTest {

    private static final String GENDER = "ОСНОВНЫЕ СВЕДЕНИЯ.Пол";
    private static final String PASSPORT_TYPE = "ДУЛ.Паспорт РФ.Тип ДУЛ";
    private static final String BIRTH_CERTIFICATE_TYPE =
            "ДУЛ.Свидетельство о рождении.Тип ДУЛ";

    private final ObjectMapper mapper = new ObjectMapper();
    private final MappingNormalizer normalizer = new MappingNormalizer(mapper);

    @Test
    void mapsAllReferenceValuesBeforeValidation() {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = root.putObject("data");

        ObjectNode baseInfo = data.putObject("baseInfo");
        baseInfo.put("gender", "female");
        baseInfo.put("mapping.gender", GENDER);

        ObjectNode documents = data.putObject("documents");
        ArrayNode cards = documents.putArray("clientIdCard");
        addDocument(cards, "PASSPORT_RU", PASSPORT_TYPE);
        addDocument(cards, "BIRTH_CERTIFICATE", BIRTH_CERTIFICATE_TYPE);

        Map<String, String> result = normalizer.normalize(
                root,
                Map.of("FEMALE", "F"),
                Map.of("PASSPORT_RU", "21", "BIRTH_CERTIFICATE", "27")
        );

        assertEquals("F", result.get(GENDER));
        assertEquals("21", result.get(PASSPORT_TYPE));
        assertEquals("27", result.get(BIRTH_CERTIFICATE_TYPE));

        // Нормализация не изменяет исходный JSON.
        assertEquals("female", baseInfo.get("gender").asText());
        assertEquals("PASSPORT_RU", cards.get(0).get("type").asText());
        assertEquals("BIRTH_CERTIFICATE", cards.get(1).get("type").asText());
    }

    @Test
    void mapsBeforeJoiningRepeatedLogicalFields() {
        ObjectNode root = mapper.createObjectNode();
        ArrayNode cards = root.putObject("data")
                .putObject("documents")
                .putArray("clientIdCard");

        String commonLogicalField = "ДУЛ.Документ.Тип ДУЛ";
        addDocument(cards, "PASSPORT_RU", commonLogicalField);
        addDocument(cards, "BIRTH_CERTIFICATE", commonLogicalField);

        Map<String, String> result = normalizer.normalize(
                root,
                Map.of(),
                Map.of("PASSPORT_RU", "21", "BIRTH_CERTIFICATE", "27")
        );

        assertEquals("21;27", result.get(commonLogicalField));
    }

    @Test
    void preservesUnknownNullAndNonMatchingValues() {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode block = root.putObject("data").putObject("block");

        block.put("unknownType", "UNKNOWN");
        block.put("mapping.unknownType", PASSPORT_TYPE);
        block.putNull("gender");
        block.put("mapping.gender", GENDER);
        block.put("series", "PASSPORT_RU");
        block.put("mapping.series", "ДУЛ.Паспорт РФ.Серия");
        block.put("ignored", "PASSPORT_RU");
        block.put("mapping.ignored", "none");

        Map<String, String> result = normalizer.normalize(
                root,
                Map.of("FEMALE", "F"),
                Map.of("PASSPORT_RU", "21")
        );

        assertEquals("UNKNOWN", result.get(PASSPORT_TYPE));
        assertNull(result.get(GENDER));
        assertEquals("PASSPORT_RU", result.get("ДУЛ.Паспорт РФ.Серия"));
        assertFalse(result.containsKey("none"));
    }

    @Test
    void trimsLogicalFieldNameAndPrioritizesExactLookup() {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode block = root.putObject("data").putObject("block");
        block.put("type", "passport_ru");
        block.put("mapping.type", "  " + PASSPORT_TYPE + "  ");

        Map<String, String> result = normalizer.normalize(
                root,
                Map.of(),
                Map.of("passport_ru", "exact", "PASSPORT_RU", "fallback")
        );

        assertEquals("exact", result.get(PASSPORT_TYPE));
        assertFalse(result.containsKey("  " + PASSPORT_TYPE + "  "));
    }

    private void addDocument(ArrayNode cards, String type, String logicalField) {
        ObjectNode card = cards.addObject();
        card.put("type", type);
        card.put("mapping.type", logicalField);
    }
}
