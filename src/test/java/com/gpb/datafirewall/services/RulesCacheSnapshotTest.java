package com.gpb.datafirewall.services;

import com.gpb.datafirewall.model.Rule;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class RulesCacheSnapshotTest {

    @Test
    void buildsRuleAndControlAreaReferencedFieldIndexes() {
        Rule first = rule(false, Set.of("A.Anchor", "A.Dependency"));
        Rule second = rule(false, Set.of("B.Anchor", "C.CrossBlock"));

        RulesCacheSnapshot snapshot = new RulesCacheSnapshot(
                Map.of("Rule1", first, "Rule2", second),
                Map.of("dataset", "area"),
                Map.of("area", Map.of(
                        "A.Anchor", Set.of("1"),
                        "B.Anchor", Set.of("Rule2"),
                        "UNUSED.Anchor", Set.of()
                )),
                Map.of(),
                Map.of(),
                Map.of("area", true),
                Map.of(),
                Map.of(),
                "1",
                "1"
        );

        assertEquals(Set.of("A.Anchor", "A.Dependency"), snapshot.referencedFieldsForRule("1"));
        assertEquals(
                Set.of("A.Anchor", "A.Dependency", "B.Anchor", "C.CrossBlock"),
                snapshot.referencedFields("area")
        );
    }

    @Test
    void replacesOnlyRequestedBundleAndPreservesTheOtherOne() {
        Rule ruleV1 = rule(false, Set.of("FIELD.V1", "DEPENDENCY.V1"));
        Rule ruleV2 = rule(true, Set.of("FIELD.V2", "DEPENDENCY.V2"));

        RulesCacheSnapshot initial = new RulesCacheSnapshot(
                Map.of("Rule1", ruleV1),
                Map.of("dataset-v1", "area-v1"),
                Map.of("area-v1", Map.of("FIELD.V1", Set.of("Rule1"))),
                Map.of("Rule1", Map.of("classification", "error")),
                Map.of("area-v1", Set.of("excluded-v1")),
                Map.of("area-v1", true),
                Map.of("MALE", "1"),
                Map.of("PASSPORT_RU", "21"),
                "1",
                "10"
        );

        RulesCacheSnapshot politicsV2 = initial.withPolitics(
                Map.of("dataset-v2", "area-v2"),
                Map.of("area-v2", Map.of("FIELD.V2", Set.of("2"))),
                Map.of("Rule2", Map.of("classification", "warning")),
                Map.of("area-v2", Set.of("excluded-v2")),
                Map.of("area-v2", false),
                Map.of("FEMALE", "2"),
                Map.of("BIRTH_CERTIFICATE", "27"),
                "20"
        );

        assertSame(initial.compiledRules(), politicsV2.compiledRules());
        assertEquals("1", politicsV2.compiledRulesVersion());
        assertEquals("area-v2", politicsV2.controlAreaByDataset("dataset-v2"));
        assertEquals("20", politicsV2.politicsVersion());
        assertSame(initial.referencedFieldsByRule(), politicsV2.referencedFieldsByRule());

        // Rule2 ещё не загружено: для совместимости со старой версией
        // snapshot оставляет хотя бы якорный атрибут politics-кэша.
        assertEquals(Set.of("FIELD.V2"), politicsV2.referencedFields("area-v2"));

        RulesCacheSnapshot rulesV2 = politicsV2.withCompiledRules(
                Map.of("Rule2", ruleV2),
                "2"
        );

        assertSame(politicsV2.dataset2ControlArea(), rulesV2.dataset2ControlArea());
        assertSame(politicsV2.controlAreaRules(), rulesV2.controlAreaRules());
        assertEquals("2", rulesV2.compiledRulesVersion());
        assertEquals("20", rulesV2.politicsVersion());
        assertEquals("area-v2", rulesV2.controlAreaByDataset("dataset-v2"));
        assertEquals(
                Set.of("FIELD.V2", "DEPENDENCY.V2"),
                rulesV2.referencedFieldsForRule("2")
        );
        assertEquals(
                Set.of("FIELD.V2", "DEPENDENCY.V2"),
                rulesV2.referencedFields("area-v2")
        );
    }

    private static Rule rule(boolean result, Set<String> referencedFields) {
        return new Rule() {
            @Override
            public boolean apply(Map<String, String> data) {
                return result;
            }

            @Override
            public Set<String> referencedFields() {
                return referencedFields;
            }
        };
    }
}
