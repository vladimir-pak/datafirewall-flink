package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.model.Rule;
import com.gpb.datafirewall.rule.RulesReloader;
import org.apache.flink.api.java.utils.ParameterTool;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** No API/HTTP/Ignite dependencies. One atomic activation per full cache epoch. */
public final class RulesCacheRuntime implements AutoCloseable {

    public static final String CACHE_COMPILED_RULES="compiled_rules";
    public static final String CACHE_POLITICS="politics";
    public static final String CACHE_GENDER_MAPPING="gender_mapping";
    public static final String CACHE_TYPE_DUL_MAPPING="type_dul_mapping";

    private final AtomicReference<RulesCacheSnapshot> active =
            new AtomicReference<>(RulesCacheSnapshot.empty());

    private final RulesReloader reloader = new RulesReloader();

    public RulesCacheRuntime(
            ObjectMapper mapper,
            String jwt) { /* compatibility */ }

    public void open(ParameterTool pt) { /* no remote initialization */ }

    public RulesCacheSnapshot prepare(CacheBundleState bundle) {
        bundle.verify();
        RulesReloader.PreparedRules prepared =
                reloader.loadFromBytecodes(
                        bundle.classNameToBytecode(),
                        bundle.compiledRulesVersion()
                );

        return new RulesCacheSnapshot(
                prepared.rules(),
                bundle.dataset2ControlArea(),
                bundle.controlAreaRules(),
                bundle.errorMessages(),
                bundle.datasetExclusion(),
                bundle.filterFlags(),
                bundle.genderMapping(),
                bundle.typeDulMapping(),
                prepared.version(),
                bundle.politicsVersion(),
                bundle.cacheEpoch()
        );
    }

    public void activate(RulesCacheSnapshot prepared) {
        if (prepared == null || prepared.cacheEpoch() < 0) 
                throw new IllegalArgumentException("Invalid snapshot");
        active.set(prepared);
    }

    public RulesCacheSnapshot snapshot() {
        return active.get();
    }

    public Map<String,Rule> rulesSnapshot() {
        return snapshot().compiledRules();
    }

    public int rulesSize() {
        return snapshot().compiledRules().size();
    }

    public String controlAreaByDataset(String ds) {
        return snapshot().controlAreaByDataset(ds);
    }

    public Map<String,Set<String>> fieldToRules(String area) {
        return snapshot().fieldToRules(area);
    }

    public Set<String> referencedFields(String area) {
        return snapshot().referencedFields(area);
    }

    public Map<String,Set<String>> referencedFieldsByRuleSnapshot() {
        return snapshot().referencedFieldsByRule();
    }

    public Map<String,Map<String,String>> errorMessagesSnapshot() {
        return snapshot().errorMessagesByRule();
    }

    public Set<String> excludedBlocks(String area) {
        return snapshot().excludedBlocks(area);
    }

    public Boolean filterFlag(String area) {
        return snapshot().filterFlag(area);
    }

    public int dataset2ControlAreaSize() {
        return snapshot().dataset2ControlArea().size();
    }

    public int controlAreaRulesSize() {
        return snapshot().controlAreaRules().size();
    }

    public int errorMessagesSize() {
        return snapshot().errorMessagesByRule().size();
    }

    public int datasetExclusionSize() {
        return snapshot().excludedBlocksByControlArea().size();
    }

    public int filterFlagSize() {
        return snapshot().filterFlagsByControlArea().size();
    }

    public Map<String,String> genderMappingSnapshot() {
        return snapshot().genderMapping();
    }

    public Map<String,String> typeDulMappingSnapshot() {
        return snapshot().typeDulMapping();
    }

    public int genderMappingSize() {
        return snapshot().genderMapping().size();
    }

    public int typeDulMappingSize() {
        return snapshot().typeDulMapping().size();
    }

    public int referencedRulesSize() {
        return snapshot().referencedFieldsByRule().size();
    }

    public int referencedControlAreasSize() {
        return snapshot().referencedFieldsByControlArea().size();
    }

    public long compiledRulesVersion() {
        return numeric(snapshot().compiledRulesVersion());
    }

    public long politicsVersion() {
        return numeric(snapshot().politicsVersion());
    }

    private static long numeric(String s) {
        try {
            return Long.parseLong(s);
        } catch (Exception ex) {
            return -1;
        }
    }
    
    @Override public void close() { /* nothing to close */ }
}
