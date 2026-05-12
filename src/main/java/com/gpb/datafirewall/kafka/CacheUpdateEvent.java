package com.gpb.datafirewall.kafka;

import java.io.Serializable;
import java.util.Objects;

public class CacheUpdateEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public final long version;
    public final String cacheName;

    public CacheUpdateEvent() {
        this.version = -1;
        this.cacheName = null;
    }

    public CacheUpdateEvent(long version, String cacheName) {
        this.version = version;
        this.cacheName = cacheName;
    }

    public boolean isValid() {
        return version >= 0
                && cacheName != null
                && !cacheName.trim().isEmpty();
    }

    @Override
    public String toString() {
        return "CacheUpdateEvent{" +
                "version=" + version +
                ", cacheName='" + cacheName + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, cacheName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CacheUpdateEvent other)) return false;
        return version == other.version &&
                Objects.equals(cacheName, other.cacheName);
    }
}