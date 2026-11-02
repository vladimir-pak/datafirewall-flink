package com.gpb.datafirewall.services;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.rule.RuleClassLoader;
import com.gpb.datafirewall.testfixtures.TestBundles;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
class CacheRecoveryAndFailureTest {
    @Test void restoresRealBytecodeWithoutApi() {
        var saved = CacheBundleStateSerializer.encode(TestBundles.bundle(42));
        var runtime = new RulesCacheRuntime(null, null);
        runtime.activate(runtime.prepare(CacheBundleStateSerializer.decode(saved)));
        var rule = runtime.snapshot().compiledRules().get(TestBundles.RULE);
        assertInstanceOf(RuleClassLoader.class, rule.getClass().getClassLoader());
        assertEquals(42, runtime.snapshot().cacheEpoch());
        assertTrue(rule.apply(Map.of("field", "bad")));
        assertFalse(rule.apply(Map.of("field", "ok")));
    }
    @Test void rejectsMissingRuleWithoutChangingActiveCache() {
        var runtime = new RulesCacheRuntime(null, null);
        runtime.activate(runtime.prepare(TestBundles.bundle(41)));
        assertThrows(IllegalStateException.class, () -> runtime.prepare(TestBundles.bundle(42, "absent")));
        assertEquals(41, runtime.snapshot().cacheEpoch());
    }
    @Test void propagatesTechnicalProcessingFailure() {
        var mapper = new ObjectMapper() {
            @Override public JsonNode readTree(String content) { throw new IllegalStateException("injected failure"); }
        };
        var service = new MessageProcessingService(mapper, new RulesCacheRuntime(null, null), false);
        assertThrows(IllegalStateException.class,
                () -> service.process(MessageRecord.fromJms("1", "{}", null, null)));
    }
}
