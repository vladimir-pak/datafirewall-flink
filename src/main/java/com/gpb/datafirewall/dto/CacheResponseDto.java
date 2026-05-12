package com.gpb.datafirewall.dto;

import java.util.Map;

public class CacheResponseDto<K, V> {

    private String cacheName;
    private int size;
    private Map<K, V> cache;

    public CacheResponseDto() {
    }

    public CacheResponseDto(String cacheName, int size, Map<K, V> cache) {
        this.cacheName = cacheName;
        this.size = size;
        this.cache = cache;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Map<K, V> getCache() {
        return cache == null ? Map.of() : cache;
    }

    public void setCache(Map<K, V> cache) {
        this.cache = cache;
    }
}