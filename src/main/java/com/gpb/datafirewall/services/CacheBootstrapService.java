package com.gpb.datafirewall.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpb.datafirewall.cache.CompiledRulesRegistry;
import com.gpb.datafirewall.cache.PoliticsControlAreaRulesCache;
import com.gpb.datafirewall.cache.PoliticsDatasetControlAreaCache;
import com.gpb.datafirewall.cache.PoliticsDatasetExclusionCache;
import com.gpb.datafirewall.cache.PoliticsErrorMessagesCache;
import com.gpb.datafirewall.cache.PoliticsFilterFlagCache;
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

    private final IgniteRulesApiClient igniteApiClient;
    private final RulesReloader rulesReloader;
    private final CompiledRulesRegistry rulesRegistry;

    private final PoliticsDatasetControlAreaCache politicsDatasetControlAreaCache;
    private final PoliticsControlAreaRulesCache politicsControlAreaRulesCache;
    private final PoliticsErrorMessagesCache politicsErrorMessagesCache;
    private final PoliticsDatasetExclusionCache politicsDatasetExclusionCache;
    private final PoliticsFilterFlagCache politicsFilterFlagCache;

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
                cachePayloadConverter.toStringMap(dataset2ControlAreaResponse.getCache(), CACHE_POLITICS_DATASET2CONTROL_AREA);

        Map<String, Map<String, Set<String>>> controlAreaRules =
                cachePayloadConverter.toNestedRulesMap(controlAreaRulesResponse.getCache(), CACHE_POLITICS_CONTROL_AREA_RULES);

        Map<String, Map<String, String>> errorMessages =
                cachePayloadConverter.toNestedStringMap(errorMessagesResponse.getCache(), CACHE_POLITICS_ERROR_MESSAGES);

        Map<String, Set<String>> datasetExclusion =
                cachePayloadConverter.toSetMap(datasetExclusionResponse.getCache(), CACHE_POLITICS_DATASET_EXCLUSION);

        Map<String, Boolean> filterFlags =
                cachePayloadConverter.toBooleanMap(filterFlagResponse.getCache(), CACHE_POLITICS_FILTER_FLAG);

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
}