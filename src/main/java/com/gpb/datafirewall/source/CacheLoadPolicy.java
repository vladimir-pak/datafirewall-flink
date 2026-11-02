package com.gpb.datafirewall.source;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.config.CacheApiException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.function.LongFunction;
import java.util.function.Supplier;

/** Явное восстановление после GC; никогда не скрывает недействительный пакет */
public final class CacheLoadPolicy {
    private CacheLoadPolicy() {}

    public static CacheBundleState load(
            long epoch, 
            LongFunction<CacheBundleState> exact,
            Supplier<CacheBundleState> latest) {

        try {
            return exact.apply(epoch);
        } catch (RuntimeException failure) {
            CacheApiException api = CacheApiException.find(failure);

            if (api == null || !api.versionUnavailable()) 
                throw failure;

            CacheBundleState replacement = latest.get();

            if (replacement.cacheEpoch() <= epoch) {
                throw new IllegalStateException("Epoch " + epoch + " is unavailable, but latest="
                        + replacement.cacheEpoch() + "; refusing an incomplete or older cache", failure);
            }
            
            return replacement;
        }
    }

    public static boolean retryable(Throwable failure) {
        CacheApiException api = CacheApiException.find(failure);
        if (api != null) return api.retryable();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof JsonProcessingException) return false;
            if (cause instanceof IOException
                    || cause instanceof org.apache.ignite.client.ClientConnectionException) return true;
        }
        return false;
    }
}
