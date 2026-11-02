package com.gpb.datafirewall.bundle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CacheBundleStateSerializerTest {

    private static CacheBundleState sample() {
        Map<String, byte[]> codes = new LinkedHashMap<>();
        codes.put("z.Rule", new byte[] {1, 2, 3});
        codes.put("a.Rule", new byte[] {4, 5});
        return CacheBundleState.create(
                "11",
                "8",
                codes,
                Map.of("DS", "CA"),
                Map.of("CA", Map.of("field", Set.of("1", "2"))),
                Map.of("r", Map.of("error", "bad")),
                Map.of("CA", Set.of("homeAddress")),
                Map.of("CA", true),
                Map.of("M", "Male"),
                Map.of("ID", "Passport"),
                "flink");
    }

    @Test
    void deterministicRoundTripAndIndependentVersions() {
        CacheBundleState initial = sample();
        assertNotEquals(initial.compiledRulesVersion(), initial.politicsVersion());
        CacheBundleState restored =
                CacheBundleStateSerializer.decode(CacheBundleStateSerializer.encode(initial));
        assertEquals(initial.compiledRulesVersion(), restored.compiledRulesVersion());
        assertEquals(initial.politicsVersion(), restored.politicsVersion());
        assertEquals(initial.checksum(), restored.checksum());
        assertEquals(initial.controlAreaRules(), restored.controlAreaRules());
        byte[] first = CacheBundleStateSerializer.encode(restored);
        assertArrayEquals(first, CacheBundleStateSerializer.encode(initial));
    }

    @Test
    void rulesUpdatePreservesIndependentPoliticsState() {
        CacheBundleState initial = sample();
        CacheBundleState changed = initial.withRules("12", Map.of("b.Rule", new byte[] {6, 7}), "flink");
        assertEquals("12", changed.compiledRulesVersion());
        assertEquals("8", changed.politicsVersion());
        assertEquals(initial.controlAreaRules(), changed.controlAreaRules());
        assertEquals(initial.dataset2ControlArea(), changed.dataset2ControlArea());
        assertNotEquals(initial.checksum(), changed.checksum());
    }

    @Test
    void bytecodesCannotBeMutatedThroughGetter() {
        CacheBundleState bundle = sample();
        bundle.classNameToBytecode().get("z.Rule")[0] = 99;
        bundle.verify();
        assertEquals(1, bundle.classNameToBytecode().get("z.Rule")[0]);
    }

    @Test
    void serializedCorruptionIsRejected() {
        byte[] corrupted = CacheBundleStateSerializer.encode(sample());
        corrupted[corrupted.length / 2] ^= 1;
        assertThrows(
                RuntimeException.class, () -> CacheBundleStateSerializer.decode(corrupted));
    }
}
