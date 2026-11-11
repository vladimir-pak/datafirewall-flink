package com.gpb.datafirewall.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gpb.datafirewall.config.CacheApiException;
import java.io.IOException;

/** Retry I/O/transient API errors only; do not hide bad versions or incompatible rules. */
public final class CacheRetryPolicy {
    private CacheRetryPolicy() {}
    public static boolean retryable(Throwable failure) {
        CacheApiException api = CacheApiException.find(failure);
        if (api != null) return api.retryable();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof JsonProcessingException) return false;
            if (current instanceof IOException
                    || current instanceof org.apache.ignite.client.ClientConnectionException) return true;
        }
        return false;
    }
}
