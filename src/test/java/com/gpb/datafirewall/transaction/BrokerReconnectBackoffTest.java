package com.gpb.datafirewall.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BrokerReconnectBackoffTest {
    @Test
    void retriesAreBoundedAndResetAfterSuccess() {
        BrokerReconnectBackoff backoff = new BrokerReconnectBackoff(5_000L, 60_000L);
        long start = TimeUnit.SECONDS.toNanos(10);
        assertTrue(backoff.canAttempt(start));
        assertEquals(5_000L, backoff.onFailure(start));
        assertFalse(backoff.canAttempt(start + TimeUnit.SECONDS.toNanos(4)));
        assertTrue(backoff.canAttempt(start + TimeUnit.SECONDS.toNanos(5)));
        assertEquals(10_000L, backoff.onFailure(start + TimeUnit.SECONDS.toNanos(5)));
        assertEquals(20_000L, backoff.onFailure(start + TimeUnit.SECONDS.toNanos(15)));
        assertEquals(40_000L, backoff.onFailure(start + TimeUnit.SECONDS.toNanos(35)));
        assertEquals(60_000L, backoff.onFailure(start + TimeUnit.SECONDS.toNanos(75)));
        assertEquals(60_000L, backoff.onFailure(start + TimeUnit.SECONDS.toNanos(135)));
        backoff.onSuccess();
        assertTrue(backoff.canAttempt(start));
        assertEquals(5_000L, backoff.onFailure(start));
    }
}
