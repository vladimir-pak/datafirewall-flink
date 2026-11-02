package com.gpb.datafirewall.bundle;

/** Неизменяемый, атомарно опубликованный манифест для одной согласованной epoch кэша. */
public record CacheManifest(
    long cacheEpoch,
    String compiledRulesVersion,
    String politicsVersion,
    String handler,
    String checksum) {

    public CacheManifest {
        if (cacheEpoch < 0
                || compiledRulesVersion == null
                || compiledRulesVersion.isBlank()
                || politicsVersion == null
                || politicsVersion.isBlank()) {
            throw new IllegalArgumentException("Incomplete cache manifest");
        }
    }
}
