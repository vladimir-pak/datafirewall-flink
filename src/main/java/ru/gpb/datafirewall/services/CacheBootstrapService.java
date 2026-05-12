package ru.gpb.datafirewall.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.gpb.datafirewall.cache.CompiledRulesRegistry;
import ru.gpb.datafirewall.cache.PoliticsControlAreaRulesCache;
import ru.gpb.datafirewall.cache.PoliticsDatasetControlAreaCache;
import ru.gpb.datafirewall.cache.PoliticsDatasetExclusionCache;
import ru.gpb.datafirewall.cache.PoliticsErrorMessagesCache;
import ru.gpb.datafirewall.cache.PoliticsFilterFlagCache;
import ru.gpb.datafirewall.config.IgniteRulesApiClient;
import ru.gpb.datafirewall.dto.CacheResponseDto;
import ru.gpb.datafirewall.rule.RulesReloader;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CacheBootstrapService {
    private static final Logger log = LoggerFactory.getLogger(CacheBootstrapService.class);

    private static final String CACHE_COMPILED_RULES = "compiled_rules";
    private static final String CACHE_POLITICS_DATASET2CONTROL_AREA = "politics_dataset2control_area";
    private static final String CACHE_POLITICS_CONTROL_AREA_RULES = "politics_control_area_rules";
    private static final String CACHE_POLITICS_ERROR_MESSAGES = "politics_error_messages";
    private static final String CACHE_POLITICS_DATASET_EXCLUSION = "politics_dataset_exclusion";
    private static final String CACHE_POLITICS_FILTER_FLAG = "politics_filter_flag";

    private final IgniteRulesApiClient igniteApiClient;
    private final RulesReloader rulesReloader;
    private final CompiledRulesRegistry rulesRegistry;

    private final PoliticsDatasetControlAreaCache politicsDatasetControlAreaCache;
    private final PoliticsControlAreaRulesCache politicsControlAreaRulesCache;
    private final PoliticsErrorMessagesCache politicsErrorMessagesCache;
    private final PoliticsDatasetExclusionCache politicsDatasetExclusionCache;
    private final PoliticsFilterFlagCache politicsFilterFlagCache;

    private final boolean politicsBootstrapEnabled;

    public CacheBootstrapService(
            IgniteRulesApiClient igniteApiClient,
            RulesReloader rulesReloader,
            CompiledRulesRegistry rulesRegistry,
            PoliticsDatasetControlAreaCache politicsDatasetControlAreaCache,
            PoliticsControlAreaRulesCache politicsControlAreaRulesCache,
            PoliticsErrorMessagesCache politicsErrorMessagesCache,
            PoliticsDatasetExclusionCache politicsDatasetExclusionCache,
            PoliticsFilterFlagCache politicsFilterFlagCache,
            boolean politicsBootstrapEnabled
    ) {
        this.igniteApiClient = igniteApiClient;
        this.rulesReloader = rulesReloader;
        this.rulesRegistry = rulesRegistry;
        this.politicsDatasetControlAreaCache = politicsDatasetControlAreaCache;
        this.politicsControlAreaRulesCache = politicsControlAreaRulesCache;
        this.politicsErrorMessagesCache = politicsErrorMessagesCache;
        this.politicsDatasetExclusionCache = politicsDatasetExclusionCache;
        this.politicsFilterFlagCache = politicsFilterFlagCache;
        this.politicsBootstrapEnabled = politicsBootstrapEnabled;
    }

    public void initializeAll() {
        long t0 = System.nanoTime();

        initCompiledRules();

        if (politicsBootstrapEnabled) {
            initPoliticsCaches();
        } else {
            log.info("[BOOTSTRAP] politics bootstrap disabled");
        }

        long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info(
                "[BOOTSTRAP] all caches initialized in {}ms: rules={}, dataset2controlArea={}, controlAreaRules={}, errorMessages={}, datasetExclusion={}, filterFlag={}",
                ms,
                rulesRegistry.size(),
                politicsDatasetControlAreaCache.size(),
                politicsControlAreaRulesCache.size(),
                politicsErrorMessagesCache.size(),
                politicsDatasetExclusionCache.size(),
                politicsFilterFlagCache.size()
        );
    }

    private void initCompiledRules() {
        CacheResponseDto<String, Object> latest = igniteApiClient.getActualCache(CACHE_COMPILED_RULES);

        String fullCacheName = latest.getCacheName();
        String version = extractVersion(fullCacheName);

        log.info("[BOOTSTRAP] loading latest compiled_rules: fullCacheName={}, version={}",
                fullCacheName, version);

        rulesReloader.reloadAllStrict(fullCacheName);

        log.info("[BOOTSTRAP] compiled_rules initialized: version={}, size={}",
                version, rulesRegistry.size());
    }

    private void initPoliticsCaches() {
        CacheResponseDto<String, Object> dataset2ControlAreaResponse =
                igniteApiClient.getActualCache(CACHE_POLITICS_DATASET2CONTROL_AREA);

        CacheResponseDto<String, Object> controlAreaRulesResponse =
                igniteApiClient.getActualCache(CACHE_POLITICS_CONTROL_AREA_RULES);

        CacheResponseDto<String, Object> errorMessagesResponse =
                igniteApiClient.getActualCache(CACHE_POLITICS_ERROR_MESSAGES);

        CacheResponseDto<String, Object> datasetExclusionResponse =
                igniteApiClient.getActualCache(CACHE_POLITICS_DATASET_EXCLUSION);

        CacheResponseDto<String, Object> filterFlagResponse =
                igniteApiClient.getActualCache(CACHE_POLITICS_FILTER_FLAG);

        String datasetVersion = extractVersion(dataset2ControlAreaResponse.getCacheName());
        String rulesVersion = extractVersion(controlAreaRulesResponse.getCacheName());
        String errorsVersion = extractVersion(errorMessagesResponse.getCacheName());
        String exclusionVersion = extractVersion(datasetExclusionResponse.getCacheName());
        String filterFlagVersion = extractVersion(filterFlagResponse.getCacheName());

        ensureSamePoliticsVersion(
                datasetVersion,
                rulesVersion,
                errorsVersion,
                exclusionVersion,
                filterFlagVersion
        );

        Map<String, String> dataset2ControlArea =
                toStringMap(dataset2ControlAreaResponse.getCache(), CACHE_POLITICS_DATASET2CONTROL_AREA);

        Map<String, Map<String, Set<String>>> controlAreaRules =
                toNestedRulesMap(controlAreaRulesResponse.getCache(), CACHE_POLITICS_CONTROL_AREA_RULES);

        Map<String, String> errorMessages =
                toStringMap(errorMessagesResponse.getCache(), CACHE_POLITICS_ERROR_MESSAGES);

        Map<String, Set<String>> datasetExclusion =
                toSetMap(datasetExclusionResponse.getCache(), CACHE_POLITICS_DATASET_EXCLUSION);

        Map<String, Boolean> filterFlags =
                toBooleanMap(filterFlagResponse.getCache(), CACHE_POLITICS_FILTER_FLAG);

        politicsDatasetControlAreaCache.replaceAll(dataset2ControlArea, datasetVersion);
        politicsControlAreaRulesCache.replaceAll(controlAreaRules, datasetVersion);
        politicsErrorMessagesCache.replaceAll(errorMessages, datasetVersion);
        politicsDatasetExclusionCache.replaceAll(datasetExclusion, datasetVersion);
        politicsFilterFlagCache.replaceAll(filterFlags, datasetVersion);

        log.info(
                "[BOOTSTRAP] politics initialized: version={}, dataset2controlArea={}, controlAreaRules={}, errorMessages={}, datasetExclusion={}, filterFlag={}",
                datasetVersion,
                dataset2ControlArea.size(),
                controlAreaRules.size(),
                errorMessages.size(),
                datasetExclusion.size(),
                filterFlags.size()
        );
    }

    private void ensureSamePoliticsVersion(
            String datasetVersion,
            String rulesVersion,
            String errorsVersion,
            String exclusionVersion,
            String filterFlagVersion
    ) {
        if (!datasetVersion.equals(rulesVersion)
                || !datasetVersion.equals(errorsVersion)
                || !datasetVersion.equals(exclusionVersion)
                || !datasetVersion.equals(filterFlagVersion)) {
            throw new IllegalStateException(
                    "Politics cache versions mismatch: " +
                            "dataset=" + datasetVersion +
                            ", rules=" + rulesVersion +
                            ", errors=" + errorsVersion +
                            ", exclusion=" + exclusionVersion +
                            ", filterFlag=" + filterFlagVersion
            );
        }
    }

    private String extractVersion(String fullCacheName) {
        if (fullCacheName == null || fullCacheName.isBlank()) {
            throw new IllegalArgumentException("fullCacheName must not be null/blank");
        }

        int idx = fullCacheName.lastIndexOf('_');
        if (idx < 0 || idx == fullCacheName.length() - 1) {
            throw new IllegalArgumentException(
                    "Cannot extract version from cache name '" + fullCacheName + "'"
            );
        }

        return fullCacheName.substring(idx + 1).trim();
    }

    private Map<String, String> toStringMap(Map<String, Object> payload, String cacheName) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }
            if (entry.getValue() == null) {
                throw new IllegalStateException(
                        "Null value for key '" + entry.getKey() + "' in cache " + cacheName
                );
            }

            result.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return result;
    }

    private Map<String, Set<String>> toSetMap(Map<String, Object> payload, String cacheName) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();
        var setType = com.fasterxml.jackson.databind.json.JsonMapper.builder().build()
                .getTypeFactory()
                .constructCollectionType(Set.class, String.class);

        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();

        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }

            Set<String> value = mapper.convertValue(entry.getValue(), setType);
            result.put(entry.getKey(), value);
        }
        return result;
    }

    private Map<String, Map<String, Set<String>>> toNestedRulesMap(
            Map<String, Object> payload,
            String cacheName
    ) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var tf = mapper.getTypeFactory();
        var setType = tf.constructCollectionType(Set.class, String.class);
        var innerMapType = tf.constructMapType(
                LinkedHashMap.class,
                tf.constructType(String.class),
                setType
        );

        Map<String, Map<String, Set<String>>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }

            Map<String, Set<String>> value = mapper.convertValue(entry.getValue(), innerMapType);
            result.put(entry.getKey(), value);
        }
        return result;
    }

    private Map<String, Boolean> toBooleanMap(Map<String, Object> payload, String cacheName) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        Map<String, Boolean> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }
            if (entry.getValue() == null) {
                throw new IllegalStateException(
                        "Null value for key '" + entry.getKey() + "' in cache " + cacheName
                );
            }

            Boolean value = mapper.convertValue(entry.getValue(), Boolean.class);
            result.put(entry.getKey(), value);
        }

        return result;
    }
}