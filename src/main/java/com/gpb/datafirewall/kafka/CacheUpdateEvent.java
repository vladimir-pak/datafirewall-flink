package com.gpb.datafirewall.kafka;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

/** Kafka cache updates: rules/politics use version >= 0; handler uses version = 0. */
public final class CacheUpdateEvent implements Serializable {
    private static final long serialVersionUID = 2L;

    public final int version;
    public final String cacheName;
    public final String handler;

    public CacheUpdateEvent(int version, String cacheName) {
        this(version, cacheName, null);
    }

    public CacheUpdateEvent(int version, String cacheName, String handler) {
        this.version = version;
        this.cacheName = cacheName == null ? null : cacheName.trim().toLowerCase(Locale.ROOT);
        this.handler = handler == null || handler.isBlank() ? null : handler.trim().toLowerCase(Locale.ROOT);
    }

    public boolean isRules() { return "rules".equals(cacheName) || "compiled_rules".equals(cacheName); }
    public boolean isPolitics() { return "politics".equals(cacheName); }
    public boolean isHandler() { return "handler".equals(cacheName); }
    public boolean isValid() {
        boolean validHandler = handler == null || "flink".equals(handler) || "dotnet".equals(handler);
        if (isHandler()) {
            return version == 0 && handler != null && validHandler;
        }
        return version >= 0 && (isRules() || isPolitics()) && validHandler;
    }

    @Override public String toString() {
        return "CacheUpdateEvent{cacheName='" + cacheName + "', version=" + version
                + ", handler='" + handler + "'}";
    }
    @Override public boolean equals(Object other) {
        return other instanceof CacheUpdateEvent event && version == event.version
                && Objects.equals(cacheName, event.cacheName)
                && Objects.equals(handler, event.handler);
    }
    @Override public int hashCode() { return Objects.hash(version, cacheName, handler); }
}
