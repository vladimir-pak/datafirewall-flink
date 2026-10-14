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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageProcessingCrossBlockRuleInputTest {

    private static final String CITIZENSHIP_COUNTRY = "ГРАЖДАНСТВО.Страна";
    private static final String ADDRESS_COUNTRY = "АДРЕС.Страна";
    private static final String ADDRESS_DISTRICT = "АДРЕС.Район";

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
                                CITIZENSHIP_COUNTRY, Set.of("RuleMainCrossBlock")
                        ),
                        "CA_ADDRESS", Map.of(
                                ADDRESS_COUNTRY, Set.of(),
                                ADDRESS_DISTRICT, Set.of("RuleAddressCrossBlock")
                        )
                ),
                Map.of(),
                Map.of("CA_MAIN", Set.of("registrationAddress")),
                Map.of("CA_MAIN", true, "CA_ADDRESS", true),
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
    void excludedRuleReceivesMissingCrossBlockDependencyAsNull() throws Exception {
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

        RulesCacheSnapshot snapshot = snapshot(mainRule, addressRule, false, false);
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
        assertTrue(addressInput.get().containsKey(CITIZENSHIP_COUNTRY));
        assertNull(addressInput.get().get(CITIZENSHIP_COUNTRY));
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

        RulesCacheSnapshot snapshot = snapshot(mainRule, addressRule, true, true);
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
                                "MAIN.Anchor", Set.of("RuleMainCrossBlock")
                        ),
                        "CA_ADDRESS", Map.of(
                                ADDRESS_COUNTRY, Set.of(),
                                ADDRESS_DISTRICT, Set.of("RuleAddressCrossBlock")
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
