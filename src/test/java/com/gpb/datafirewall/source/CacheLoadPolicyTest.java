package com.gpb.datafirewall.source;
import com.gpb.datafirewall.config.CacheApiException;
import com.gpb.datafirewall.testfixtures.TestBundles;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;
class CacheLoadPolicyTest {
    @Test void movesForwardWhenOldVersionWasRemoved() {
        var result = CacheLoadPolicy.load(72, e -> { throw new CacheApiException(410, "version"); },
                () -> TestBundles.bundle(75));
        assertEquals(75, result.cacheEpoch());
    }
    @Test void refusesSameOrOlderReplacement() {
        assertThrows(IllegalStateException.class, () -> CacheLoadPolicy.load(72,
                e -> { throw new CacheApiException(404, "version"); }, () -> TestBundles.bundle(71)));
    }
    @Test void neverHidesCorruptionOrAuthenticationFailure() {
        for (RuntimeException ex : new RuntimeException[]{new IllegalStateException("checksum mismatch"),
                new CacheApiException(401, "url")}) {
            assertSame(ex, assertThrows(RuntimeException.class, () -> CacheLoadPolicy.load(72,
                    e -> { throw ex; }, () -> { fail("latest must not be called"); return null; })));
            assertFalse(CacheLoadPolicy.retryable(ex));
        }
    }
    @Test void retriesOnlyTemporaryFailures() {
        assertTrue(CacheLoadPolicy.retryable(new RuntimeException(new IOException("offline"))));
        assertTrue(CacheLoadPolicy.retryable(new CacheApiException(503, "url")));
        assertFalse(CacheLoadPolicy.retryable(new CacheApiException(404, "url")));
    }
}
