package com.gpb.datafirewall.bundle;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class CacheBundleState implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final int SCHEMA_VERSION = 2;
    private final int schemaVersion;
    private final String compiledRulesVersion;
    private final String politicsVersion;
    private final Map<String, byte[]> classNameToBytecode;
    private final Map<String, String> dataset2ControlArea;
    private final Map<String, Map<String, Set<String>>> controlAreaRules;
    private final Map<String, Map<String, String>> errorMessages;
    private final Map<String, Set<String>> datasetExclusion;
    private final Map<String, Boolean> filterFlags;
    private final Map<String, String> genderMapping;
    private final Map<String, String> typeDulMapping;
    private final String handler;
    private final String checksum;

    CacheBundleState(
            int schemaVersion, 
            String compiledRulesVersion,
            String politicsVersion, 
            Map<String, byte[]> classNameToBytecode,
            Map<String, String> dataset2ControlArea,
            Map<String, Map<String, Set<String>>> controlAreaRules,
            Map<String, Map<String, String>> errorMessages,
            Map<String, Set<String>> datasetExclusion, 
            Map<String, Boolean> filterFlags,
            Map<String, String> genderMapping, 
            Map<String, String> typeDulMapping,
            String handler, 
            String checksum) {

        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported bundle schema " + schemaVersion
            );
        }

        this.schemaVersion = schemaVersion;

        this.compiledRulesVersion = required(
                compiledRulesVersion, 
                "compiledRulesVersion"
        );

        this.politicsVersion = required(
                politicsVersion, 
                "politicsVersion"
        );

        Objects.requireNonNull(
                classNameToBytecode, 
                "classNameToBytecode"
        );

        if (classNameToBytecode.isEmpty()) 
            throw new IllegalArgumentException("Empty compiled rules bundle");

        Map<String, byte[]> copied = new LinkedHashMap<>();

        classNameToBytecode.forEach((k,v) -> {
            required(k, "rule class name");

            if (v == null || v.length == 0) 
                throw new IllegalArgumentException("Empty bytecode for " + k);

            copied.put(k, v.clone());
        });

        this.classNameToBytecode = Collections.unmodifiableMap(copied);
        this.dataset2ControlArea = freeze(dataset2ControlArea);
        this.controlAreaRules = freezeNestedSets(controlAreaRules);
        this.errorMessages = freezeNested(errorMessages);
        this.datasetExclusion = freezeSets(datasetExclusion);
        this.filterFlags = freeze(filterFlags);
        this.genderMapping = freeze(genderMapping);
        this.typeDulMapping = freeze(typeDulMapping);
        this.handler = handler == null ? "flink" : handler.trim();

        if (!this.handler.equals("flink") 
                && !this.handler.equals("dotnet"))
            throw new IllegalArgumentException("Unsupported handler: " + this.handler);

        this.checksum = checksum;
    }

    public static CacheBundleState create(
            String compiledVersion, 
            String politicsVersion,
            Map<String, byte[]> bytecodes, 
            Map<String, String> dataset,
            Map<String, Map<String, 
            Set<String>>> rules,
            Map<String, Map<String, String>> errors, 
            Map<String, Set<String>> exclusions,
            Map<String, Boolean> flags, 
            Map<String, String> genders,
            Map<String, String> duls, 
            String handler) {

        CacheBundleState unsigned = 
                new CacheBundleState(
                        SCHEMA_VERSION, 
                        compiledVersion,
                        politicsVersion, 
                        bytecodes, 
                        dataset, 
                        rules, 
                        errors, 
                        exclusions, 
                        flags, 
                        genders, 
                        duls, 
                        handler, 
                        null
                );

        return unsigned.withChecksum(
                CacheBundleStateSerializer.computeChecksum(unsigned)
        );
    }

    /** Replace only bytecode, preserving every policy and dictionary from this immutable bundle. */
    public CacheBundleState withRules(String rulesVersion, Map<String, byte[]> codes, String newHandler) {
        return create(rulesVersion, politicsVersion, codes,
                dataset2ControlArea, controlAreaRules, errorMessages, datasetExclusion,
                filterFlags, genderMapping, typeDulMapping, newHandler);
    }

    /** Used only to compose a subsequent full cache update, not on handler-only notifications. */
    public CacheBundleState withHandler(String nextHandler) {
        if (handler.equals(nextHandler)) return this;
        return create(compiledRulesVersion, politicsVersion, classNameToBytecode,
                dataset2ControlArea, controlAreaRules, errorMessages, datasetExclusion,
                filterFlags, genderMapping, typeDulMapping, nextHandler);
    }

    CacheBundleState withChecksum(String sum) {
        return new CacheBundleState(
                schemaVersion, 
                compiledRulesVersion, 
                politicsVersion,
                classNameToBytecode, 
                dataset2ControlArea, 
                controlAreaRules, 
                errorMessages,
                datasetExclusion, 
                filterFlags, 
                genderMapping, 
                typeDulMapping, 
                handler, 
                sum
        );
    }

    public void verify() {
        if (checksum == null 
                || !checksum.equalsIgnoreCase(
                        CacheBundleStateSerializer.computeChecksum(this)
                )
        )
            throw new IllegalStateException(
                    "Cache bundle checksum mismatch, rules=" + compiledRulesVersion
                            + ", politics=" + politicsVersion
            );
    }

    private static String required(
            String value, 
            String what) {
        if (value == null || value.isBlank()) 
            throw new IllegalArgumentException("Missing " + what);

        return value;
    }

    private static <V> Map<String,V> freeze(Map<String,V> m) {
        return Collections.unmodifiableMap(
                new LinkedHashMap<>(
                        Objects.requireNonNull(m, "cache map")
                )
        );
    }

    private static Map<String, Set<String>> freezeSets(
            Map<String, Set<String>> m) {

        Map<String, Set<String>> out = new LinkedHashMap<>();

        Objects.requireNonNull(m, "set map").forEach((k,v) ->
                out.put(
                        k, 
                        Collections.unmodifiableSet(
                                new LinkedHashSet<>(Objects.requireNonNull(v, k))
                        )
                )
        );

        return Collections.unmodifiableMap(out);
    }

    private static Map<String, Map<String,String>> freezeNested(
            Map<String,Map<String,String>> m) {

        Map<String,Map<String,String>> out = new LinkedHashMap<>();

        Objects.requireNonNull(m, "nested map").forEach((k,v) -> 
                out.put(k, freeze(v))
        );

        return Collections.unmodifiableMap(out);
    }

    private static Map<String, Map<String,Set<String>>> freezeNestedSets(
            Map<String,Map<String,Set<String>>> m) {

        Map<String,Map<String,Set<String>>> out = new LinkedHashMap<>();

        Objects.requireNonNull(m, "nested set map").forEach((k,v) -> 
                out.put(k, freezeSets(v))
        );
        return Collections.unmodifiableMap(out);
    }

    public int schemaVersion() { 
        return schemaVersion; 
    }
    
    public String compiledRulesVersion() { 
        return compiledRulesVersion; 
    }

    public String politicsVersion() { 
        return politicsVersion; 
    }

    /** Returns defensive copies: byte[] values must not be mutated by recipients. */
    public Map<String,byte[]> classNameToBytecode() {
        Map<String,byte[]> out = new LinkedHashMap<>();
        classNameToBytecode.forEach((k,v) -> out.put(k, v.clone()));
        return out;
    }

    public Map<String,String> dataset2ControlArea() { 
        return dataset2ControlArea; 
    }

    public Map<String,Map<String,Set<String>>> controlAreaRules() { 
        return controlAreaRules; 
    }

    public Map<String,Map<String,String>> errorMessages() { 
        return errorMessages; 
    }

    public Map<String,Set<String>> datasetExclusion() { 
        return datasetExclusion; 
    }

    public Map<String,Boolean> filterFlags() { 
        return filterFlags; 
    }

    public Map<String,String> genderMapping() { 
        return genderMapping; 
    }

    public Map<String,String> typeDulMapping() { 
        return typeDulMapping; 
    }

    public String handler() { 
        return handler; 
    }

    public String checksum() { 
        return checksum; 
    }
}
