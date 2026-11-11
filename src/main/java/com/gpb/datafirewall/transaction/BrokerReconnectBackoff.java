package com.gpb.datafirewall.transaction;

import java.util.concurrent.TimeUnit;

/** Simple bounded exponential backoff; owned by one receiver thread. */
public final class BrokerReconnectBackoff {
    private final long initialMs;
    private final long maxMs;
    private long delayMs;
    private long nextAttemptNanos;

    public BrokerReconnectBackoff(long initialMs, long maxMs) {
        if (initialMs <= 0 || maxMs < initialMs) {
            throw new IllegalArgumentException("Invalid broker reconnect backoff");
        }
        this.initialMs = initialMs;
        this.maxMs = maxMs;
    }

    public boolean canAttempt(long nowNanos) {
        return nextAttemptNanos == 0 || nowNanos - nextAttemptNanos >= 0;
    }

    public long onFailure(long nowNanos) {
        delayMs = delayMs == 0 ? initialMs : Math.min(maxMs, delayMs >= maxMs / 2 ? maxMs : delayMs * 2);
        nextAttemptNanos = nowNanos + TimeUnit.MILLISECONDS.toNanos(delayMs);
        return delayMs;
    }

    public void onSuccess() {
        delayMs = 0;
        nextAttemptNanos = 0;
    }
}
