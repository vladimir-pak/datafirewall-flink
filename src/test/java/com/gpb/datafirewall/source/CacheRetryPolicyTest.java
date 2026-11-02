package com.gpb.datafirewall.source;
import com.gpb.datafirewall.config.CacheApiException;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;
class CacheRetryPolicyTest {
    @Test void rejectsNonTransientErrors() {
        assertFalse(CacheRetryPolicy.retryable(new IllegalStateException("checksum mismatch")));
        assertFalse(CacheRetryPolicy.retryable(new CacheApiException(401, "url")));
        assertFalse(CacheRetryPolicy.retryable(new CacheApiException(404, "url")));
    }
    @Test void retriesTransientFailures() {
        assertTrue(CacheRetryPolicy.retryable(new RuntimeException(new IOException("offline"))));
        assertTrue(CacheRetryPolicy.retryable(new CacheApiException(503, "url")));
    }
}
