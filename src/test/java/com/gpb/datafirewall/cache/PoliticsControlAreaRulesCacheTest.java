package com.gpb.datafirewall.cache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PoliticsControlAreaRulesCacheTest {

    @Test
    void preparesNaturalFieldAndRuleOrderOnceDuringPublication() {
        Map<String, Set<String>> fields = new LinkedHashMap<>();
        fields.put("B.FIELD", new LinkedHashSet<>(List.of("Rule20", "Rule10")));
        fields.put("A.FIELD", new LinkedHashSet<>(List.of("Rule2", "Rule1")));

        PoliticsControlAreaRulesCache cache = new PoliticsControlAreaRulesCache();
        cache.replaceAll(Map.of("AREA", fields), "1");

        Map<String, Set<String>> snapshot = cache.get("AREA");

        assertEquals(List.of("A.FIELD", "B.FIELD"), new ArrayList<>(snapshot.keySet()));
        assertEquals(List.of("Rule1", "Rule2"), new ArrayList<>(snapshot.get("A.FIELD")));
        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.put("C.FIELD", Set.of("Rule3")));
        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.get("A.FIELD").add("Rule3"));
    }
}
