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

        assertEquals(1, calls.get());
        assertFalse(normalized.containsKey(TYPE));
        assertEquals("SUCCESS", result.allResult());
        assertEquals("OK", result.processStatus());
        assertFalse(result.detailByField().containsKey(SERIES));
        assertFalse(result.detailByDataset().containsKey(SERIES));
        assertFalse(result.errorsByField().containsKey(SERIES));
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
