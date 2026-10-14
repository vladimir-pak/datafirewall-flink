package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.model.Rule;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageProcessingArrayPreparationTest {

    @Test
    @SuppressWarnings("unchecked")
    void preparesEveryArrayItemOnceAndKeepsPreviousSeparationSemantics() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        MessageProcessingService service = new MessageProcessingService(mapper, null, false);

        ObjectNode data = mapper.createObjectNode();
        data.putObject("main").put("dataset_code", "ROOT_DS");

        CountingArrayNode documents = new CountingArrayNode();
        data.set("documents", documents);
        documents.addObject()
                .put("dataset_code", "DS_1")
                .put("value", "first")
                .put("mapping.value", "ARRAY.ONE");
        documents.add("ignored scalar");
        documents.addObject()
                .put("dataset_code", "DS_2")
                .put("value", "second")
                .put("mapping.value", "ARRAY.TWO");

        Map<String, Set<String>> rootRules = Map.of(
                "ROOT.FIELD", Set.of("RootRule"),
                "ARRAY.ONE", Set.of("Rule1"),
                "ARRAY.TWO", Set.of("Rule2"),
                "ARRAY.MISSING", Set.of("Rule3")
        );
        Map<String, Set<String>> firstRules = Map.of(
                "ARRAY.ONE", Set.of("Rule1")
        );
        Map<String, Set<String>> secondRules = Map.of(
                "ARRAY.TWO", Set.of("Rule2"),
                "ARRAY.MISSING", Set.of("Rule3")
        );

        RulesCacheSnapshot snapshot = new RulesCacheSnapshot(
                Map.of(
                        "RootRule", rule(Set.of("ROOT.FIELD")),
                        "Rule1", rule(Set.of("ARRAY.ONE")),
                        "Rule2", rule(Set.of("ARRAY.TWO", "ARRAY.MISSING")),
                        "Rule3", rule(Set.of("ARRAY.MISSING"))
                ),
                Map.of(
                        "ROOT_DS", "ROOT_AREA",
                        "DS_1", "AREA_1",
                        "DS_2", "AREA_2"
                ),
                Map.of(
                        "ROOT_AREA", rootRules,
                        "AREA_1", Map.of("WRONG.ONE", Set.of()),
                        "AREA_2", Map.of("WRONG.TWO", Set.of())
                ),
                Map.of(),
                Map.of(),
                Map.of("ROOT_AREA", true, "AREA_1", false, "AREA_2", false),
                Map.of(),
                Map.of(),
                "1",
                "1"
        );

        Method prepare = MessageProcessingService.class.getDeclaredMethod(
                "prepareTopLevelBlocks",
                JsonNode.class,
                String.class,
                Map.class,
                Boolean.class,
                Map.class,
                Map.class
        );
        prepare.setAccessible(true);

        Object result = prepare.invoke(
                service,
                data,
                "ROOT_DS",
                rootRules,
                true,
                Map.of(),
                Map.of()
        );

        Map<String, JsonNode> objectBlocks = (Map<String, JsonNode>) component(result, "objectBlocks");
        List<Object> items = (List<Object>) component(result, "arrayItems");
        Set<String> separatedFields = (Set<String>) component(result, "arrayLogicalFields");

        assertEquals(Set.of("main"), objectBlocks.keySet());
        assertEquals(2, items.size());
        assertEquals(1, documents.iterations());
        assertTrue(separatedFields.containsAll(Set.of("ARRAY.ONE", "ARRAY.TWO")));
        assertFalse(separatedFields.contains("ARRAY.MISSING"));
        assertFalse(separatedFields.contains("ROOT.FIELD"));

        Object first = items.get(0);
        assertEquals("DS_1#documents#0", component(first, "instanceKey"));
        assertEquals(Map.of("ARRAY.ONE", "first"), component(first, "normalizedMap"));
        assertEquals(firstRules, component(first, "fieldToRules"));

        Object second = items.get(1);
        assertEquals("DS_2#documents#2", component(second, "instanceKey"));

        Map<String, String> secondNormalized =
                (Map<String, String>) component(second, "normalizedMap");
        Map<String, Set<String>> secondSelectedRules =
                (Map<String, Set<String>>) component(second, "fieldToRules");

        assertEquals("second", secondNormalized.get("ARRAY.TWO"));
        assertFalse(secondNormalized.containsKey("ARRAY.MISSING"));
        assertEquals(Set.of("ARRAY.TWO"), secondSelectedRules.keySet());
    }

    private static Rule rule(Set<String> referencedFields) {
        return new Rule() {
            @Override
            public boolean apply(Map<String, String> data) {
                return false;
            }

            @Override
            public Set<String> referencedFields() {
                return referencedFields;
            }
        };
    }

    private static Object component(Object record, String name) throws Exception {
        Method accessor = record.getClass().getDeclaredMethod(name);
        accessor.setAccessible(true);
        return accessor.invoke(record);
    }

    private static final class CountingArrayNode extends ArrayNode {
        private int iterations;

        private CountingArrayNode() {
            super(JsonNodeFactory.instance);
        }

        @Override
        public Iterator<JsonNode> elements() {
            iterations++;
            return super.elements();
        }

        private int iterations() {
            return iterations;
        }
    }
}
