package com.gpb.datafirewall.bundle;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class CacheBundleEnvelopeTest {
    @Test void handlerOnlyEnvelopeDoesNotContainCache() {
        CacheBundleEnvelope event = CacheBundleEnvelope.forHandler("dotnet");
        event.validateHeader();
        assertTrue(event.handlerOnly);
        assertEquals("dotnet", event.handler);
        assertNull(event.bytes);
        assertNull(event.rulesVersion);
        assertNull(event.politicsVersion);
        assertNull(event.checksum);
    }

    @Test void rejectsHandlerEventWithCacheBytes() {
        CacheBundleEnvelope event = CacheBundleEnvelope.forHandler("flink");
        event.bytes = new byte[] {1};
        assertThrows(IllegalArgumentException.class, event::validateHeader);
    }
}
