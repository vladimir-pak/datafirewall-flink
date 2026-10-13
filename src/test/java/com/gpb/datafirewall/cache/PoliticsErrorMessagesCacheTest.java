package com.gpb.datafirewall.cache;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PoliticsErrorMessagesCacheTest {

    @Test
    void publishesDeeplyImmutableSnapshot() {
        Map<String, String> errorInfo = new LinkedHashMap<>();
        errorInfo.put("classification", "error");

        Map<String, Map<String, String>> source = new LinkedHashMap<>();
        source.put("Rule1", errorInfo);

        PoliticsErrorMessagesCache cache = new PoliticsErrorMessagesCache();
        cache.replaceAll(source, "1");

        errorInfo.put("classification", "warning");
        source.put("Rule2", Map.of());

        assertEquals("error", cache.snapshot().get("Rule1").get("classification"));
        assertEquals(1, cache.snapshot().size());
        assertThrows(
                UnsupportedOperationException.class,
                () -> cache.snapshot().get("Rule1").put("classification", "warning")
        );
    }
}
