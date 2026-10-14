package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.model.Rule;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageProcessingCrossBlockRuleInputTest {

    private static final String CITIZENSHIP_COUNTRY = "ГРАЖДАНСТВО.Страна";
    private static final String ADDRESS_COUNTRY = "АДРЕС.Страна";
    private static final String ADDRESS_DISTRICT = "АДРЕС.Район";
    private static final String BIRTHDATE = "ОСНОВНЫЕ СВЕДЕНИЯ.Дата рождения";
    private static final String SNILS = "ОСНОВНЫЕ СВЕДЕНИЯ.СНИЛС";

    @Test
    void mainAndExcludedRulesSeeFieldsFromBothBlocks() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = root.putObject("data");

        data.putObject("citizenship")
                .put("dataset_code", "MAIN_DS")
                .put("country", "Россия")
                .put("mapping.country", CITIZENSHIP_COUNTRY);

        data.putObject("registrationAddress")
                .put("dataset_code", "ADDRESS_DS")
                .put("countryName", "Страна")
                .put("mapping.countryName", ADDRESS_COUNTRY)
                .put("district", "Район")
                .put("mapping.district", ADDRESS_DISTRICT);

        AtomicInteger mainCalls = new AtomicInteger();
        AtomicInteger addressCalls = new AtomicInteger();
        AtomicReference<Map<String, String>> mainInput = new AtomicReference<>();
        AtomicReference<Map<String, String>> addressInput = new AtomicReference<>();

        Rule mainRule = rule(Set.of(CITIZENSHIP_COUNTRY, ADDRESS_COUNTRY), values -> {
            mainCalls.incrementAndGet();
            mainInput.set(new LinkedHashMap<>(values));
            required(values, CITIZENSHIP_COUNTRY);
            required(values, ADDRESS_COUNTRY);
            return false;
        });

        Rule addressRule = rule(
                Set.of(ADDRESS_DISTRICT, ADDRESS_COUNTRY, CITIZENSHIP_COUNTRY),
                values -> {
            addressCalls.incrementAndGet();
            addressInput.set(new LinkedHashMap<>(values));
            required(values, ADDRESS_DISTRICT);
            required(values, ADDRESS_COUNTRY);
            required(values, CITIZENSHIP_COUNTRY);
            return false;
        });

        RulesCacheSnapshot snapshot = new RulesCacheSnapshot(
                Map.of(
                        "RuleMainCrossBlock", mainRule,
                        "RuleAddressCrossBlock", addressRule
                ),
                Map.of(
                        "MAIN_DS", "CA_MAIN",
                        "ADDRESS_DS", "CA_ADDRESS"
                ),
                Map.of(
                        "CA_MAIN", Map.of(
                                CITIZENSHIP_COUNTRY, Set.of("RuleMainCrossBlock"),
                                ADDRESS_COUNTRY, Set.of(),
                                ADDRESS_DISTRICT, Set.of("RuleAddressCrossBlock")
                        ),
                        "CA_ADDRESS", Map.of(
                                "WRONG.FIELD", Set.of()
                        )
                ),
                Map.of(),
                Map.of("CA_MAIN", Set.of("registrationAddress")),
                Map.of("CA_MAIN", true, "CA_ADDRESS", false),
                Map.of(),
                Map.of(),
                "1",
                "1"
        );

        RulesCacheRuntime runtime = runtimeWithSnapshot(mapper, snapshot);
        MessageProcessingService service = new MessageProcessingService(mapper, runtime, false);

        ProcessingResult result = service.process(MessageRecord.fromJms(
                "ID:cross-block",
                mapper.writeValueAsString(root),
                1L,
                2L
        ));

        assertNotNull(result);
        assertEquals(1, mainCalls.get());
        assertEquals(1, addressCalls.get());

        assertEquals("Россия", mainInput.get().get(CITIZENSHIP_COUNTRY));
        assertEquals("Страна", mainInput.get().get(ADDRESS_COUNTRY));

        assertEquals("Россия", addressInput.get().get(CITIZENSHIP_COUNTRY));
        assertEquals("Страна", addressInput.get().get(ADDRESS_COUNTRY));
        assertEquals("Район", addressInput.get().get(ADDRESS_DISTRICT));
    }

    @Test
    void excludedRulePrefersLocalValueOverCommonMessageValue() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = root.putObject("data");

        data.putObject("baseInfo")
                .put("dataset_code", "MAIN_DS")
                .put("citizenship", "Россия")
                .put("mapping.citizenship", CITIZENSHIP_COUNTRY);

        data.putObject("registrationAddress")
                .put("dataset_code", "ADDRESS_DS")
                .put("countryName", "Страна")
                .put("mapping.countryName", ADDRESS_COUNTRY)
                .put("district", "Район")
                .put("mapping.district", ADDRESS_DISTRICT)
                .put("citizenship", "Казахстан")
                .put("mapping.citizenship", CITIZENSHIP_COUNTRY);

        AtomicInteger addressCalls = new AtomicInteger();
        AtomicReference<Map<String, String>> addressInput = new AtomicReference<>();
        Rule addressRule = rule(
                Set.of(ADDRESS_DISTRICT, CITIZENSHIP_COUNTRY),
                values -> {
                    addressCalls.incrementAndGet();
                    addressInput.set(new LinkedHashMap<>(values));
                    required(values, ADDRESS_DISTRICT);
                    required(values, CITIZENSHIP_COUNTRY);
                    return false;
                }
        );

        RulesCacheSnapshot snapshot = new RulesCacheSnapshot(
                Map.of("RuleAddress", addressRule),
                Map.of(
                        "MAIN_DS", "CA_MESSAGE",
                        "ADDRESS_DS", "CA_IGNORED"
                ),
                Map.of(
                        "CA_MESSAGE", Map.of(
                                ADDRESS_COUNTRY, Set.of(),
                                ADDRESS_DISTRICT, Set.of("RuleAddress")
                        ),
                        "CA_IGNORED", Map.of("WRONG.FIELD", Set.of())
                ),
                Map.of(),
                Map.of("CA_MESSAGE", Set.of("registrationAddress")),
                Map.of("CA_MESSAGE", false, "CA_IGNORED", true),
                Map.of(),
                Map.of(),
                "1",
                "1"
        );

        RulesCacheRuntime runtime = runtimeWithSnapshot(mapper, snapshot);
        MessageProcessingService service =
                new MessageProcessingService(mapper, runtime, false);

        ProcessingResult result = service.process(MessageRecord.fromJms(
                "ID:local-value-priority",
                mapper.writeValueAsString(root),
                1L,
                2L
        ));

        assertNotNull(result);
        assertEquals(1, addressCalls.get());
        assertNotNull(addressInput.get());
        assertEquals("Казахстан", addressInput.get().get(CITIZENSHIP_COUNTRY));
    }

    @Test
    void excludedRuleDoesNotReceiveSyntheticMissingDependency() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = root.putObject("data");

        data.putObject("main")
                .put("dataset_code", "MAIN_DS")
                .put("anchor", "present")
                .put("mapping.anchor", "MAIN.Anchor");

        data.putObject("registrationAddress")
                .put("dataset_code", "ADDRESS_DS")
                .put("countryName", "Страна")
                .put("mapping.countryName", ADDRESS_COUNTRY)
                .put("district", "Район")
                .put("mapping.district", ADDRESS_DISTRICT);

        AtomicReference<Map<String, String>> addressInput = new AtomicReference<>();

        Rule mainRule = rule(Set.of("MAIN.Anchor"), values -> false);
        Rule addressRule = rule(
                Set.of(ADDRESS_DISTRICT, ADDRESS_COUNTRY, CITIZENSHIP_COUNTRY),
                values -> {
                    addressInput.set(new LinkedHashMap<>(values));
                    required(values, ADDRESS_DISTRICT);
                    required(values, ADDRESS_COUNTRY);
                    required(values, CITIZENSHIP_COUNTRY);
                    return false;
                }
        );

        RulesCacheSnapshot snapshot = snapshot(mainRule, addressRule, false, true);
        RulesCacheRuntime runtime = runtimeWithSnapshot(mapper, snapshot);
        MessageProcessingService service = new MessageProcessingService(mapper, runtime, false);

        ProcessingResult result = service.process(MessageRecord.fromJms(
                "ID:missing-cross-block",
                mapper.writeValueAsString(root),
                1L,
                2L
        ));

        assertNotNull(result);
        assertNotNull(addressInput.get());
        assertFalse(addressInput.get().containsKey(CITIZENSHIP_COUNTRY));
        assertEquals(
                "OK",
                mapper.readTree(result.getShortJson())
                        .path("data")
                        .path("PROCESS_STATUS")
                        .asText()
        );
    }

    @Test
    void syntheticReferencedFieldDoesNotStartRuleOfMissingAnchor() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = root.putObject("data");

        data.putObject("main")
                .put("dataset_code", "MAIN_DS")
                .put("anchor", "present")
                .put("mapping.anchor", "MAIN.Anchor");

        data.putObject("registrationAddress")
                .put("dataset_code", "ADDRESS_DS")
                .put("countryName", "Страна")
                .put("mapping.countryName", ADDRESS_COUNTRY);

        AtomicInteger addressCalls = new AtomicInteger();
        Rule mainRule = rule(Set.of("MAIN.Anchor"), values -> false);
        Rule addressRule = rule(
                Set.of(ADDRESS_DISTRICT, ADDRESS_COUNTRY, CITIZENSHIP_COUNTRY),
                values -> {
                    addressCalls.incrementAndGet();
                    return false;
                }
        );

        RulesCacheSnapshot snapshot = snapshot(mainRule, addressRule, true, false);
        RulesCacheRuntime runtime = runtimeWithSnapshot(mapper, snapshot);
        MessageProcessingService service = new MessageProcessingService(mapper, runtime, false);

        ProcessingResult result = service.process(MessageRecord.fromJms(
                "ID:missing-anchor",
                mapper.writeValueAsString(root),
                1L,
                2L
        ));

        assertNotNull(result);
        assertEquals(0, addressCalls.get());
    }

    @Test
    void excludedBlockDoesNotMaskPresentMainFieldWhenControlAreaIsShared()
            throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = root.putObject("data");

        data.putObject("baseInfo")
                .put("dataset_code", "MAIN_DS")
                .put("birthdate", "2000-01-01")
                .put("mapping.birthdate", BIRTHDATE);

        data.putObject("registrationAddress")
                .put("dataset_code", "ADDRESS_DS")
                .put("countryName", "РОССИЯ")
                .put("mapping.countryName", ADDRESS_COUNTRY);

        ObjectNode documents = data.putObject("documents");
        documents.put("dataset_code", "DOCUMENTS_DS");
        documents.putNull("clientSnils");
        documents.put("mapping.clientSnils", SNILS);

        AtomicInteger birthdateCalls = new AtomicInteger();
        AtomicReference<Map<String, String>> birthdateInput = new AtomicReference<>();
        AtomicInteger addressCalls = new AtomicInteger();
        AtomicReference<Map<String, String>> addressInput = new AtomicReference<>();

        Rule birthdateRule = rule(Set.of(BIRTHDATE), values -> {
            birthdateCalls.incrementAndGet();
            birthdateInput.set(new LinkedHashMap<>(values));
            return required(values, BIRTHDATE) == null;
        });

        Rule addressRule = rule(Set.of(ADDRESS_DISTRICT, BIRTHDATE), values -> {
            addressCalls.incrementAndGet();
            addressInput.set(new LinkedHashMap<>(values));
            return required(values, ADDRESS_DISTRICT) == null;
        });

        RulesCacheSnapshot snapshot = new RulesCacheSnapshot(
                Map.of(
                        "RuleBirthdate", birthdateRule,
                        "RuleAddress", addressRule
                ),
                Map.of(
                        "MAIN_DS", "CA_SHARED",
                        "ADDRESS_DS", "CA_SHARED",
                        "DOCUMENTS_DS", "CA_SHARED"
                ),
                Map.of("CA_SHARED", Map.of(
                        BIRTHDATE, Set.of("RuleBirthdate"),
                        ADDRESS_COUNTRY, Set.of(),
                        ADDRESS_DISTRICT, Set.of("RuleAddress"),
                        SNILS, Set.of()
                )),
                Map.of(),
                Map.of("CA_SHARED", Set.of("registrationAddress", "documents")),
                Map.of("CA_SHARED", false),
                Map.of(),
                Map.of(),
                "1",
                "1"
        );

        RulesCacheRuntime runtime = runtimeWithSnapshot(mapper, snapshot);
        MessageProcessingService service = new MessageProcessingService(mapper, runtime, false);

        ProcessingResult result = service.process(MessageRecord.fromJms(
                "ID:shared-control-area",
                mapper.writeValueAsString(root),
                1L,
                2L
        ));

        assertNotNull(result);
        assertEquals(1, birthdateCalls.get());
        assertNotNull(birthdateInput.get());
        assertEquals("2000-01-01", birthdateInput.get().get(BIRTHDATE));
        assertEquals(1, addressCalls.get());
        assertNotNull(addressInput.get());
        assertFalse(addressInput.get().containsKey(ADDRESS_DISTRICT));
        assertEquals("2000-01-01", addressInput.get().get(BIRTHDATE));
    }

    @Test
    void filterFlagTrueRemovesRuleWithMissingDependencyFromBothAnswers()
            throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = root.putObject("data");

        data.putObject("main")
                .put("dataset_code", "MAIN_DS")
                .put("anchor", "present")
                .put("mapping.anchor", "MAIN.Anchor");

        data.putObject("registrationAddress")
                .put("dataset_code", "ADDRESS_DS")
                .put("district", "Район")
                .put("mapping.district", ADDRESS_DISTRICT);

        AtomicInteger calls = new AtomicInteger();
        Rule mainRule = rule(Set.of("MAIN.Anchor"), values -> false);
        Rule addressRule = rule(
                Set.of(ADDRESS_DISTRICT, CITIZENSHIP_COUNTRY),
                values -> {
                    calls.incrementAndGet();
                    required(values, ADDRESS_DISTRICT);
                    required(values, CITIZENSHIP_COUNTRY);
                    return false;
                }
        );

        RulesCacheSnapshot snapshot = snapshot(mainRule, addressRule, true, false);
        RulesCacheRuntime runtime = runtimeWithSnapshot(mapper, snapshot);
        MessageProcessingService service =
                new MessageProcessingService(mapper, runtime, false);

        ProcessingResult result = service.process(MessageRecord.fromJms(
                "ID:filtered-missing-dependency",
                mapper.writeValueAsString(root),
                1L,
                2L
        ));

        assertNotNull(result);
        assertEquals(1, calls.get());

        com.fasterxml.jackson.databind.JsonNode shortAnswer =
                mapper.readTree(result.getShortJson());
        assertFalse(shortAnswer.path("data").path("details")
                .has("registrationAddress"));

        com.fasterxml.jackson.databind.JsonNode detailAnswer =
                mapper.readTree(result.getDetailJson());
        com.fasterxml.jackson.databind.JsonNode addressDetail =
                detailAnswer.path("detail_results").path("ADDRESS_DS");
        assertFalse(addressDetail.has(ADDRESS_DISTRICT));
        assertEquals("SUCCESS", addressDetail.path("ALL_RESULT").asText());
    }

    private static RulesCacheSnapshot snapshot(
            Rule mainRule,
            Rule addressRule,
            boolean mainFilterFlag,
            boolean addressFilterFlag
    ) {
        return new RulesCacheSnapshot(
                Map.of(
                        "RuleMainCrossBlock", mainRule,
                        "RuleAddressCrossBlock", addressRule
                ),
                Map.of(
                        "MAIN_DS", "CA_MAIN",
                        "ADDRESS_DS", "CA_ADDRESS"
                ),
                Map.of(
                        "CA_MAIN", Map.of(
                                "MAIN.Anchor", Set.of("RuleMainCrossBlock"),
                                ADDRESS_COUNTRY, Set.of(),
                                ADDRESS_DISTRICT, Set.of("RuleAddressCrossBlock")
                        ),
                        "CA_ADDRESS", Map.of(
                                "WRONG.FIELD", Set.of()
                        )
                ),
                Map.of(),
                Map.of("CA_MAIN", Set.of("registrationAddress")),
                Map.of(
                        "CA_MAIN", mainFilterFlag,
                        "CA_ADDRESS", addressFilterFlag
                ),
                Map.of(),
                Map.of(),
                "1",
                "1"
        );
    }

    @SuppressWarnings("unchecked")
    private static RulesCacheRuntime runtimeWithSnapshot(
            ObjectMapper mapper,
            RulesCacheSnapshot snapshot
    ) throws Exception {
        RulesCacheRuntime runtime = new RulesCacheRuntime(mapper, null);
        Field field = RulesCacheRuntime.class.getDeclaredField("activeSnapshot");
        field.setAccessible(true);
        AtomicReference<RulesCacheSnapshot> reference =
                (AtomicReference<RulesCacheSnapshot>) field.get(runtime);
        reference.set(snapshot);
        return runtime;
    }

    private static String required(Map<String, String> values, String field) {
        if (!values.containsKey(field)) {
            throw new java.util.NoSuchElementException("Missing field: " + field);
        }
        return values.get(field);
    }

    private static Rule rule(
            Set<String> referencedFields,
            Predicate<Map<String, String>> predicate
    ) {
        return new Rule() {
            @Override
            public boolean apply(Map<String, String> data) {
                return predicate.test(data);
            }

            @Override
            public Set<String> referencedFields() {
                return referencedFields;
            }
        };
    }
}
