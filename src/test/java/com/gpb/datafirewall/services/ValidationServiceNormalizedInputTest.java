package com.gpb.datafirewall.services;

import com.gpb.datafirewall.model.Rule;
import com.gpb.datafirewall.validation.ValidationResult;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ValidationServiceNormalizedInputTest {

    private static final String TYPE = "ДУЛ.Паспорт РФ.Тип ДУЛ";
    private static final String SERIES = "ДУЛ.Паспорт РФ.Серия";

    @Test
    void ruleAttachedToSeriesSeesAlreadyMappedDocumentType() {
        AtomicReference<Map<String, String>> captured = new AtomicReference<>();

        Rule rule10220 = data -> {
            captured.set(new LinkedHashMap<>(data));
            return "21".equals(required(data, TYPE))
                    && required(data, SERIES).startsWith("00");
        };

        ValidationResult result = new ValidationService().validate(
                Map.of("Rule10220", rule10220),
                Map.of(TYPE, "21", SERIES, "0000"),
                Map.of(SERIES, Set.of("Rule10220")),
                Map.of()
        );

        assertEquals("21", captured.get().get(TYPE));
        assertEquals("0000", captured.get().get(SERIES));
        assertEquals("ERROR", result.allResult());
        assertEquals("ERROR", result.detailByField().get(SERIES).get("Rule10220"));
        assertEquals("OK", result.processStatus());
    }

    @Test
    void missingDependencyIsSuccessWhenFilterFlagIsFalse() {
        Map<String, String> normalized = new LinkedHashMap<>();
        normalized.put(SERIES, "0000");

        Rule rule10220 = ruleThatRequiresType(new AtomicInteger());

        ValidationResult result = new ValidationService().validate(
                Map.of("Rule10220", rule10220),
                normalized,
                Map.of(SERIES, Set.of("Rule10220")),
                Map.of(),
                false
        );

        assertFalse(normalized.containsKey(TYPE));
        assertEquals("SUCCESS", result.allResult());
        assertEquals("SUCCESS", result.detailByField().get(SERIES).get("Rule10220"));
        assertEquals("OK", result.processStatus());
    }

    @Test
    void missingDependencyIsRemovedWhenFilterFlagIsTrue() {
        Map<String, String> normalized = new LinkedHashMap<>();
        normalized.put(SERIES, "0000");
        AtomicInteger calls = new AtomicInteger();

        ValidationResult result = new ValidationService().validate(
                Map.of("Rule10220", ruleThatRequiresType(calls)),
                normalized,
                Map.of(SERIES, Set.of("Rule10220")),
                Map.of(),
                true
        );

        assertEquals(0, calls.get());
        assertFalse(normalized.containsKey(TYPE));
        assertEquals("SUCCESS", result.allResult());
        assertEquals("OK", result.processStatus());
        assertFalse(result.detailByField().containsKey(SERIES));
        assertFalse(result.detailByDataset().containsKey(SERIES));
        assertFalse(result.errorsByField().containsKey(SERIES));
    }

    @Test
    void filterTrueSkipsPermissiveRuleBeforeApply() {
        AtomicInteger calls = new AtomicInteger();
        Rule permissive = new Rule() {
            public Set<String> referencedFields() { return Set.of(TYPE, SERIES); }
            public boolean apply(Map<String, String> data) {
                calls.incrementAndGet();
                return "0000".equals(data.get(SERIES));
            }
        };
        ValidationService service = new ValidationService();
        Map<String, String> input = Map.of(SERIES, "0000");
        var filtered = service.validate(Map.of("Rule1", permissive), input,
                Map.of(SERIES, Set.of("Rule1")), Map.of(), true);
        assertEquals(0, calls.get());
        assertFalse(filtered.detailByField().containsKey(SERIES));
        assertEquals(Map.of(), filtered.errorsByField());
        var unfiltered = service.validate(Map.of("Rule1", permissive), input,
                Map.of(SERIES, Set.of("Rule1")), Map.of(), false);
        assertEquals(1, calls.get());
        assertEquals("ERROR", unfiltered.detailByField().get(SERIES).get("Rule1"));
        assertFalse(input.containsKey(TYPE));
    }

    @Test
    void presentNullIsNotFilteredOut() {
        AtomicInteger calls = new AtomicInteger();
        Rule rule = new Rule() {
            public Set<String> referencedFields() { return Set.of(TYPE); }
            public boolean apply(Map<String, String> data) {
                calls.incrementAndGet();
                return data.get(TYPE) == null;
            }
        };
        Map<String, String> input = new LinkedHashMap<>();
        input.put(TYPE, null);
        var result = new ValidationService().validate(Map.of("Rule1", rule), input,
                Map.of(TYPE, Set.of("Rule1")), Map.of(), true);
        assertEquals(1, calls.get());
        assertEquals("ERROR", result.detailByField().get(TYPE).get("Rule1"));
        assertEquals("OK", result.processStatus());
    }

    @Test
    void dependencyInFinalEnrichedInputAllowsExecution() {
        AtomicInteger calls = new AtomicInteger();
        var result = new ValidationService().validate(
                Map.of("Rule10220", ruleThatRequiresType(calls)),
                Map.of(SERIES, "0000", TYPE, "21"),
                Map.of(SERIES, Set.of("Rule10220")), Map.of(), true);
        assertEquals(1, calls.get());
        assertEquals("ERROR", result.detailByField().get(SERIES).get("Rule10220"));
    }

    private Rule ruleThatRequiresType(AtomicInteger calls) {
        return new Rule() {
            @Override
            public boolean apply(Map<String, String> data) {
                calls.incrementAndGet();
                return "21".equals(required(data, TYPE))
                        && required(data, SERIES).startsWith("00");
            }

            @Override
            public Set<String> referencedFields() {
                return Set.of(TYPE, SERIES);
            }
        };
    }

    private String required(Map<String, String> data, String key) {
        if (!data.containsKey(key)) {
            throw new java.util.NoSuchElementException("Missing field: " + key);
        }
        return data.get(key);
    }
}
