package com.gpb.datafirewall.kafka;

import com.gpb.datafirewall.services.DynamicHandler;

import java.io.Serializable;
import java.util.Objects;

public class CacheUpdateEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public final long version;
    public final String cacheName;
    public final String handler;

    public CacheUpdateEvent() {
        this.version = -1;
        this.cacheName = null;
        this.handler = null;
    }

    public CacheUpdateEvent(long version, String cacheName) {
        this(version, cacheName, null);
    }

    public CacheUpdateEvent(long version, String cacheName, String handler) {
        this.version = version;
        this.cacheName = cacheName;
        this.handler = handler == null ? null : handler.trim();
    }

    public boolean isValid() {
        return version >= 0
                && cacheName != null
                && !cacheName.trim().isEmpty();
    }

    public DynamicHandler handlerAsEnum(DynamicHandler defaultValue) {
        return DynamicHandler.from(handler, defaultValue);
    }

    @Override
    public String toString() {
        return "CacheUpdateEvent{" +
                "version=" + version +
                ", cacheName='" + cacheName + '\'' +
                ", handler='" + handler + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, cacheName, handler);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CacheUpdateEvent other)) return false;
        return version == other.version &&
                Objects.equals(cacheName, other.cacheName) &&
                Objects.equals(handler, other.handler);
    }
}
