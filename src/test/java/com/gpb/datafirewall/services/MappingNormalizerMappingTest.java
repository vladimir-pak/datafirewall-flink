package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void keepsParentMappingsBeforeNestedMappingsWhenNestedNodeAppearsFirst() {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode block = root.putObject("data").putObject("block");

        ObjectNode nested = block.putObject("nested");
        nested.put("value", "child");
        nested.put("mapping.value", "COMMON.FIELD");

        block.put("value", "parent");
        block.put("mapping.value", "COMMON.FIELD");

        Map<String, String> result = normalizer.normalize(root);

        assertEquals("parent;child", result.get("COMMON.FIELD"));
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

    @Test
    void addsAllMissingControlAreaFieldsAsNullWhenFilterFlagIsFalse() {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode passport = root.putObject("data").putObject("documents");
        passport.put("series", "0000");
        passport.put("mapping.series", "ДУЛ.Паспорт РФ.Серия");

        Map<String, Set<String>> fieldToRules = Map.of(
                "ДУЛ.Паспорт РФ.Серия", Set.of("Rule10220"),
                PASSPORT_TYPE, Set.of("Rule10220"),
                "ДУЛ.Паспорт РФ.Номер", Set.of("RuleOther")
        );

        Map<String, String> result = normalizer.normalize(
                root,
                Map.of(),
                Map.of("PASSPORT_RU", "21"),
                fieldToRules,
                false
        );

        assertEquals("0000", result.get("ДУЛ.Паспорт РФ.Серия"));
        assertTrue(result.containsKey(PASSPORT_TYPE));
        assertNull(result.get(PASSPORT_TYPE));
        assertTrue(result.containsKey("ДУЛ.Паспорт РФ.Номер"));
        assertNull(result.get("ДУЛ.Паспорт РФ.Номер"));
    }

    @Test
    void doesNotAddMissingControlAreaFieldsWhenFilterFlagIsTrue() {
        ObjectNode root = mapper.createObjectNode();
        root.putObject("data").putObject("documents");

        Map<String, String> result = normalizer.normalize(
                root,
                Map.of(),
                Map.of(),
                Map.of(PASSPORT_TYPE, Set.of("Rule10220")),
                true
        );

        assertFalse(result.containsKey(PASSPORT_TYPE));
    }

    @Test
    void doesNotAddMissingControlAreaFieldsWhenFilterFlagIsNull() {
        ObjectNode root = mapper.createObjectNode();
        root.putObject("data").putObject("documents");

        Map<String, String> result = normalizer.normalize(
                root,
                Map.of(),
                Map.of(),
                Map.of(PASSPORT_TYPE, Set.of("Rule10220")),
                null
        );

        assertFalse(result.containsKey(PASSPORT_TYPE));
    }

    @Test
    void selectsRulesOnlyForFieldsPresentBeforeNullCompletion() {
        String series = "ДУЛ.Паспорт РФ.Серия";
        Map<String, String> normalized = new java.util.LinkedHashMap<>();
        normalized.put(series, "0000");

        Map<String, Set<String>> presentFieldToRules =
                normalizer.completeMissingFieldsAndSelectPresent(
                        normalized,
                        Map.of(
                                series, Set.of("Rule10220"),
                                PASSPORT_TYPE, Set.of("RuleMissingType")
                        )
                );

        assertTrue(normalized.containsKey(PASSPORT_TYPE));
        assertNull(normalized.get(PASSPORT_TYPE));
        assertTrue(presentFieldToRules.containsKey(series));
        assertFalse(presentFieldToRules.containsKey(PASSPORT_TYPE));
    }

    @Test
    void separateSelectionDoesNotTreatReferencedNullAsPresent() {
        String series = "ДУЛ.Паспорт РФ.Серия";
        Map<String, String> normalized = new java.util.LinkedHashMap<>();
        normalized.put(series, "0000");

        Map<String, Set<String>> fieldToRules = Map.of(
                series, Set.of("Rule10220"),
                PASSPORT_TYPE, Set.of("RuleMissingType")
        );

        Map<String, Set<String>> present =
                normalizer.selectPresentFields(normalized, fieldToRules);

        assertEquals(Set.of(series), present.keySet());
        assertFalse(normalized.containsKey(PASSPORT_TYPE));

        normalizer.completeMissingFields(normalized, Set.of(PASSPORT_TYPE));

        assertTrue(normalized.containsKey(PASSPORT_TYPE));
        assertNull(normalized.get(PASSPORT_TYPE));
        assertFalse(present.containsKey(PASSPORT_TYPE));
    }

    @Test
    void localCompletionDoesNotAddCrossBlockDependencyToOverlay() {
        String addressCountry = "АДРЕС.Страна";
        String citizenshipCountry = "ГРАЖДАНСТВО.Страна";
        Map<String, String> local = new java.util.LinkedHashMap<>();

        normalizer.completeMissingLocalFields(
                local,
                Set.of(addressCountry, citizenshipCountry),
                Set.of(addressCountry)
        );

        assertTrue(local.containsKey(addressCountry));
        assertNull(local.get(addressCountry));
        assertFalse(local.containsKey(citizenshipCountry));
    }

    private void addDocument(ArrayNode cards, String type, String logicalField) {
        ObjectNode card = cards.addObject();
        card.put("type", type);
        card.put("mapping.type", logicalField);
    }
}
