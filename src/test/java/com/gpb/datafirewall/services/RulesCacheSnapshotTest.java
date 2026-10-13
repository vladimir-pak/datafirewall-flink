package com.gpb.datafirewall.services;

import com.gpb.datafirewall.model.Rule;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class RulesCacheSnapshotTest {

    @Test
    void replacesOnlyRequestedBundleAndPreservesTheOtherOne() {
        Rule ruleV1 = data -> false;
        Rule ruleV2 = data -> true;

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
                Map.of("area-v2", Map.of("FIELD.V2", Set.of("Rule2"))),
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

        RulesCacheSnapshot rulesV2 = politicsV2.withCompiledRules(
                Map.of("Rule2", ruleV2),
                "2"
        );

        assertSame(politicsV2.dataset2ControlArea(), rulesV2.dataset2ControlArea());
        assertSame(politicsV2.controlAreaRules(), rulesV2.controlAreaRules());
        assertEquals("2", rulesV2.compiledRulesVersion());
        assertEquals("20", rulesV2.politicsVersion());
        assertEquals("area-v2", rulesV2.controlAreaByDataset("dataset-v2"));
    }
}
