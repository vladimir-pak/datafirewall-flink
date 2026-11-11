package com.gpb.datafirewall.services;

import java.util.Objects;

/** One immutable publication unit: rules, dictionaries, indexes, versions and routing. */
public record PreparedRuntime(RulesCacheSnapshot snapshot, DynamicHandler handler) {
    public PreparedRuntime {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(handler, "handler");
    }
    public String compiledRulesVersion() { return snapshot.compiledRulesVersion(); }
    public String politicsVersion() { return snapshot.politicsVersion(); }
}
