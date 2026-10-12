package com.gpb.datafirewall.services;

import com.gpb.datafirewall.model.Rule;
import com.gpb.datafirewall.validation.ValidationResult;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
                Map.of(),
                false
        );

        assertEquals("21", captured.get().get(TYPE));
        assertEquals("0000", captured.get().get(SERIES));
        assertEquals("ERROR", result.allResult());
        assertEquals("ERROR", result.detailByField().get(SERIES).get("Rule10220"));
        assertEquals("OK", result.processStatus());
    }

    private String required(Map<String, String> data, String key) {
        if (!data.containsKey(key)) {
            throw new java.util.NoSuchElementException("Missing field: " + key);
        }
        return data.get(key);
    }
}
