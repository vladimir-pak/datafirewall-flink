package com.gpb.datafirewall.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpb.datafirewall.cache.CompiledRulesRegistry;
import com.gpb.datafirewall.cache.GenderMappingCache;
import com.gpb.datafirewall.cache.PoliticsControlAreaRulesCache;
import com.gpb.datafirewall.cache.PoliticsDatasetControlAreaCache;
import com.gpb.datafirewall.cache.PoliticsDatasetExclusionCache;
import com.gpb.datafirewall.cache.PoliticsErrorMessagesCache;
import com.gpb.datafirewall.cache.PoliticsFilterFlagCache;
import com.gpb.datafirewall.cache.TypeDulMappingCache;
import com.gpb.datafirewall.config.IgniteRulesApiClient;
import com.gpb.datafirewall.converter.CachePayloadConverter;
import com.gpb.datafirewall.dto.CacheResponseDto;
import com.gpb.datafirewall.rule.RulesReloader;

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
    private static final String GENDER_MAPPING = RulesCacheRuntime.CACHE_GENDER_MAPPING;
    private static final String TYPE_DUL_MAPPING = RulesCacheRuntime.CACHE_TYPE_DUL_MAPPING;

    private final IgniteRulesApiClient igniteApiClient;
    private final RulesReloader rulesReloader;
    private final CompiledRulesRegistry rulesRegistry;

    private final PoliticsDatasetControlAreaCache politicsDatasetControlAreaCache;
    private final PoliticsControlAreaRulesCache politicsControlAreaRulesCache;
    private final PoliticsErrorMessagesCache politicsErrorMessagesCache;
    private final PoliticsDatasetExclusionCache politicsDatasetExclusionCache;
    private final PoliticsFilterFlagCache politicsFilterFlagCache;
    private final GenderMappingCache genderMappingCache;
    private final TypeDulMappingCache typeDulMappingCache;

    private final CachePayloadConverter cachePayloadConverter;

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
            GenderMappingCache genderMappingCache,
            TypeDulMappingCache typeDulMappingCache,
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
        this.genderMappingCache = genderMappingCache;
        this.typeDulMappingCache = typeDulMappingCache;

        this.cachePayloadConverter = new CachePayloadConverter(new com.fasterxml.jackson.databind.ObjectMapper());
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
                "[BOOTSTRAP] all caches initialized in {}ms: rules={}, "
                + "dataset2controlArea={}, "
                + "controlAreaRules={}, "
                + "errorMessages={}, "
                + "datasetExclusion={}, "
                + "filterFlag={}, "
                + "genderMapping={}, "
                + "typeDulMapping={}",
                ms,
                rulesRegistry.size(),
                politicsDatasetControlAreaCache.size(),
                politicsControlAreaRulesCache.size(),
                politicsErrorMessagesCache.size(),
                politicsDatasetExclusionCache.size(),
                politicsFilterFlagCache.size(),
                genderMappingCache.size(),
                typeDulMappingCache.size()
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

        CacheResponseDto<String, Object> genderMappingResponse =
                igniteApiClient.getActualCache(GENDER_MAPPING);

        CacheResponseDto<String, Object> typeDulMappingResponse =
                igniteApiClient.getActualCache(TYPE_DUL_MAPPING);

        String datasetVersion = extractVersion(dataset2ControlAreaResponse.getCacheName());
        String rulesVersion = extractVersion(controlAreaRulesResponse.getCacheName());
        String errorsVersion = extractVersion(errorMessagesResponse.getCacheName());
        String exclusionVersion = extractVersion(datasetExclusionResponse.getCacheName());
        String filterFlagVersion = extractVersion(filterFlagResponse.getCacheName());
        String genderMappingVersion = extractVersion(genderMappingResponse.getCacheName());
        String typeDulMappingVersion = extractVersion(typeDulMappingResponse.getCacheName());

        ensureSamePoliticsVersion(
                datasetVersion,
                rulesVersion,
                errorsVersion,
                exclusionVersion,
                filterFlagVersion,
                genderMappingVersion,
                typeDulMappingVersion
        );

        Map<String, String> dataset2ControlArea =
                cachePayloadConverter.toStringMap(dataset2ControlAreaResponse.getCache(), CACHE_POLITICS_DATASET2CONTROL_AREA);

        Map<String, Map<String, Set<String>>> controlAreaRules =
                cachePayloadConverter.toNestedRulesMap(controlAreaRulesResponse.getCache(), CACHE_POLITICS_CONTROL_AREA_RULES);

        Map<String, Map<String, String>> errorMessages =
                cachePayloadConverter.toNestedStringMap(errorMessagesResponse.getCache(), CACHE_POLITICS_ERROR_MESSAGES);

        Map<String, Set<String>> datasetExclusion =
                cachePayloadConverter.toSetMap(datasetExclusionResponse.getCache(), CACHE_POLITICS_DATASET_EXCLUSION);

        Map<String, Boolean> filterFlags =
                cachePayloadConverter.toBooleanMap(filterFlagResponse.getCache(), CACHE_POLITICS_FILTER_FLAG);

        Map<String, String> genderMapping =
                cachePayloadConverter.toStringMap(genderMappingResponse.getCache(), GENDER_MAPPING);
        
        Map<String, String> typeDulMapping =
                cachePayloadConverter.toStringMap(typeDulMappingResponse.getCache(), TYPE_DUL_MAPPING);  

        politicsDatasetControlAreaCache.replaceAll(dataset2ControlArea, datasetVersion);
        politicsControlAreaRulesCache.replaceAll(controlAreaRules, datasetVersion);
        politicsErrorMessagesCache.replaceAll(errorMessages, datasetVersion);
        politicsDatasetExclusionCache.replaceAll(datasetExclusion, datasetVersion);
        politicsFilterFlagCache.replaceAll(filterFlags, datasetVersion);
        genderMappingCache.replaceAll(genderMapping, datasetVersion);
        typeDulMappingCache.replaceAll(typeDulMapping, datasetVersion);

        log.info(
                "[BOOTSTRAP] politics initialized: version={}, "
                + "dataset2controlArea={}, "
                + "controlAreaRules={}, "
                + "errorMessages={}, "
                + "datasetExclusion={}, "
                + "filterFlag={} "
                + "genderMapping={} "
                + "typeDulMapping={}",
                datasetVersion,
                dataset2ControlArea.size(),
                controlAreaRules.size(),
                errorMessages.size(),
                datasetExclusion.size(),
                filterFlags.size(),
                genderMapping.size(),
                typeDulMapping.size()
        );
    }

    private void ensureSamePoliticsVersion(
            String datasetVersion,
            String rulesVersion,
            String errorsVersion,
            String exclusionVersion,
            String filterFlagVersion,
            String genderMappingVersion,
            String typeDulMappingVersion
    ) {
        if (!datasetVersion.equals(rulesVersion)
                || !datasetVersion.equals(errorsVersion)
                || !datasetVersion.equals(exclusionVersion)
                || !datasetVersion.equals(filterFlagVersion)
                || !datasetVersion.equals(genderMappingVersion)
                || !datasetVersion.equals(typeDulMappingVersion)) {
            throw new IllegalStateException(
                    "Politics cache versions mismatch: " +
                            "dataset=" + datasetVersion +
                            ", rules=" + rulesVersion +
                            ", errors=" + errorsVersion +
                            ", exclusion=" + exclusionVersion +
                            ", filterFlag=" + filterFlagVersion +
                            ", genderMappingVersion=" + genderMappingVersion +
                            ", typeDulMappingVersion=" + typeDulMappingVersion
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

        String version = fullCacheName.substring(idx + 1).trim();
        if (version.isEmpty()) {
            throw new IllegalArgumentException("Empty version in cache name '" + fullCacheName + "'");
        }
        return version;
    }
}
