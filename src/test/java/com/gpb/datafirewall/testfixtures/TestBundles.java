package com.gpb.datafirewall.testfixtures;
import com.gpb.datafirewall.bundle.CacheBundleState;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
public final class TestBundles {
    public static final String RULE = "com.gpb.datafirewall.testfixtures.RuleRestore";
    public static CacheBundleState bundle(long epoch) { return bundle(epoch, RULE); }
    public static CacheBundleState bundle(long epoch, String referencedRule) {
        try (var input = RuleFixture.class.getResourceAsStream("RuleFixture.class")) {
            // Same-length constant-pool replacement: restored class is absent from the classpath.
            byte[] bytes = input.readAllBytes();
            byte[] from = "RuleFixture".getBytes(StandardCharsets.UTF_8);
            byte[] to = "RuleRestore".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i <= bytes.length - from.length; i++) {
                boolean match = true;
                for (int j = 0; j < from.length; j++) if (bytes[i+j] != from[j]) match = false;
                if (match) System.arraycopy(to, 0, bytes, i, to.length);
            }
            return CacheBundleState.create(epoch, "11", "8", Map.of(RULE, bytes),
                    Map.of("DS", "CA"), Map.of("CA", Map.of("field", Set.of(referencedRule))),
                    Map.of(), Map.of(), Map.of("CA", false), Map.of(), Map.of(), "flink");
        } catch (IOException ex) { throw new IllegalStateException(ex); }
    }
}
