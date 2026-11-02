package com.gpb.datafirewall.services;

import java.util.Objects;

/** One immutable publication unit: rules, dictionaries, indexes, epoch and routing. */
public record RuntimeEpoch(RulesCacheSnapshot snapshot, DynamicHandler handler) {
    public RuntimeEpoch {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(handler, "handler");
    }
    public long cacheEpoch() { return snapshot.cacheEpoch(); }
}
