package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.config.IgniteRulesApiClient;
import com.gpb.datafirewall.converter.CachePayloadConverter;
import com.gpb.datafirewall.dto.CacheResponseDto;
import com.gpb.datafirewall.dto.HttpBytecodeSource;
import com.gpb.datafirewall.ignite.BytecodeSource;
import com.gpb.datafirewall.kafka.CacheUpdateEvent;
import java.util.Map;
import java.util.Objects;

/** Loads versioned cache parts. Only CacheSnapshotSource calls this API-facing class. */
public final class CacheBootstrapService {
    private static final String RULES_PREFIX = "compiled_rules_";
    private static final String POLITICS_LATEST = "politics_dataset2control_area";

    private final IgniteRulesApiClient api;
    private final CachePayloadConverter converter = new CachePayloadConverter(new ObjectMapper());
    private final BytecodeSource bytecodeSource;

    public CacheBootstrapService(IgniteRulesApiClient api, BytecodeSource bytecodeSource) {
        this.api = Objects.requireNonNull(api);
        this.bytecodeSource = Objects.requireNonNull(bytecodeSource);
    }

    public static CacheBootstrapService http(IgniteRulesApiClient api) {
        return new CacheBootstrapService(api, new HttpBytecodeSource(api));
    }

    /**
     * Discover independent latest versions. Kafka position MUST be captured before this method.
     * API /latest must return the concrete versioned cache name in cacheName.
     */
    public CacheBundleState loadLatest(String handler) {
        int rulesVersion = latestVersion("compiled_rules", RULES_PREFIX);
        int politicsVersion = latestVersion(POLITICS_LATEST, POLITICS_LATEST + "_");
        return loadVersions(rulesVersion, politicsVersion, handler);
    }

    public CacheBundleState applyUpdate(CacheBundleState active, CacheUpdateEvent event) {
        Objects.requireNonNull(active, "active");
        Objects.requireNonNull(event, "event");
        if (!event.isValid()) throw new IllegalArgumentException("Invalid cache update " + event);
        if (event.isHandler()) {
            throw new IllegalArgumentException("Handler-only updates do not load caches");
        }

        int current = event.isRules()
                ? Integer.parseInt(active.compiledRulesVersion())
                : Integer.parseInt(active.politicsVersion());
        if (event.version <= current) {
            return active;
        }
        String handler = event.handler == null || event.handler.isBlank()
                ? active.handler() : event.handler;

        CacheBundleState next;
        if (event.isRules()) {
            next = active.withRules(Integer.toString(event.version),
                    bytecodes(Integer.toString(event.version)), handler);
        } else {
            String politics = Integer.toString(event.version);
            next = make(active.compiledRulesVersion(), politics,
                    active.classNameToBytecode(), handler);
        }
        next.verify();
        return next;
    }

    private CacheBundleState loadVersions(int compiled, int politics, String handler) {
        CacheBundleState bundle = make(Integer.toString(compiled), Integer.toString(politics),
                bytecodes(Integer.toString(compiled)), handler);
        validate(bundle);
        return bundle;
    }

    private CacheBundleState make(String compiled, String politics, Map<String, byte[]> bytecodes,
                                   String handler) {
        return CacheBundleState.create(compiled, politics, bytecodes,
                converter.toStringMap(cache("politics_dataset2control_area_", politics), "dataset2controlArea"),
                converter.toNestedRulesMap(cache("politics_control_area_rules_", politics), "controlAreaRules"),
                converter.toNestedStringMap(cache("politics_error_messages_", politics), "errorMessages"),
                converter.toSetMap(cache("politics_dataset_exclusion_", politics), "datasetExclusion"),
                converter.toBooleanMap(cache("politics_filter_flag_", politics), "filterFlag"),
                converter.toStringMap(cache("gender_mapping_", politics), "genderMapping"),
                converter.toStringMap(cache("type_dul_mapping_", politics), "typeDulMapping"), handler);
    }

    private Map<String, byte[]> bytecodes(String version) {
        Map<String, byte[]> codes = bytecodeSource.loadAll(RULES_PREFIX + version);
        if (codes == null || codes.isEmpty()) throw new IllegalStateException("Empty compiled rules " + version);
        return codes;
    }

    private int latestVersion(String baseName, String expectedPrefix) {
        CacheResponseDto<String, Object> result = api.getActualCache(baseName);
        String fullName = result == null ? null : result.getCacheName();
        if (fullName == null || !fullName.startsWith(expectedPrefix)) {
            throw new IllegalStateException("Expected /latest to return a concrete " + expectedPrefix
                    + "<version> cacheName, actual=" + fullName);
        }
        try {
            int version = Integer.parseInt(fullName.substring(expectedPrefix.length()));
            if (version < 0) throw new NumberFormatException("Negative version");
            return version;
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Invalid latest cache name: " + fullName, ex);
        }
    }

    private Map<String, Object> cache(String prefix, String version) {
        String fullName = prefix + version;
        CacheResponseDto<String, Object> response = api.getVersionedCache(fullName);
        if (response == null || response.getCache() == null
                || !fullName.equals(response.getCacheName())) {
            throw new IllegalStateException("Missing or unexpected versioned cache " + fullName);
        }
        return response.getCache();
    }

    public static void validate(CacheBundleState bundle) {
        bundle.verify();
        // Check references and generated classes before publishing to all subtasks.
        new RulesCacheRuntime(null, null).prepare(bundle);
    }
}
