package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.cache.CompiledRulesRegistry;
import com.gpb.datafirewall.cache.PoliticsControlAreaRulesCache;
import com.gpb.datafirewall.cache.PoliticsDatasetControlAreaCache;
import com.gpb.datafirewall.cache.PoliticsDatasetExclusionCache;
import com.gpb.datafirewall.cache.PoliticsErrorMessagesCache;
import com.gpb.datafirewall.cache.PoliticsFilterFlagCache;
import com.gpb.datafirewall.config.IgniteRulesApiClient;
import com.gpb.datafirewall.converter.CachePayloadConverter;
import com.gpb.datafirewall.dto.CacheResponseDto;
import com.gpb.datafirewall.dto.HttpBytecodeSource;
import com.gpb.datafirewall.dto.IgniteBytecodeSource;
import com.gpb.datafirewall.ignite.BytecodeSource;
import com.gpb.datafirewall.ignite.IgniteClientFacade;
import com.gpb.datafirewall.ignite.impl.IgniteClientFacadeImpl;
import com.gpb.datafirewall.kafka.CacheUpdateEvent;
import com.gpb.datafirewall.model.Rule;
import com.gpb.datafirewall.rule.RulesReloader;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class RulesCacheRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RulesCacheRuntime.class);

    public static final String CACHE_COMPILED_RULES = "compiled_rules";
    public static final String CACHE_POLITICS = "politics";

    private static final String CACHE_POLITICS_DATASET2CONTROL_AREA =
            "politics_dataset2control_area";
    private static final String CACHE_POLITICS_CONTROL_AREA_RULES =
            "politics_control_area_rules";
    private static final String CACHE_POLITICS_ERROR_MESSAGES =
            "politics_error_messages";
    private static final String CACHE_POLITICS_DATASET_EXCLUSION =
            "politics_dataset_exclusion";
    private static final String CACHE_POLITICS_FILTER_FLAG =
            "politics_filter_flag";

    private final ObjectMapper mapper;
    private final String jwt;

    private CompiledRulesRegistry rulesRegistry;
    private RulesReloader reloader;
    private BytecodeSource bytecodeSource;
    private AutoCloseable closeable;
    private IgniteRulesApiClient igniteApiClient;
    private CachePayloadConverter cachePayloadConverter;

    private PoliticsDatasetControlAreaCache dataset2ControlAreaCache;
    private PoliticsControlAreaRulesCache controlAreaRulesCache;
    private PoliticsErrorMessagesCache errorMessagesCache;
    private PoliticsDatasetExclusionCache datasetExclusionCache;
    private PoliticsFilterFlagCache filterFlagCache;

    public RulesCacheRuntime(ObjectMapper mapper, String jwt) {
        this.mapper = mapper;
        this.jwt = jwt;
    }

    public void open(ParameterTool pt) {
        this.cachePayloadConverter = new CachePayloadConverter(mapper);

        this.rulesRegistry = new CompiledRulesRegistry();
        this.dataset2ControlAreaCache = new PoliticsDatasetControlAreaCache();
        this.controlAreaRulesCache = new PoliticsControlAreaRulesCache();
        this.errorMessagesCache = new PoliticsErrorMessagesCache();
        this.datasetExclusionCache = new PoliticsDatasetExclusionCache();
        this.filterFlagCache = new PoliticsFilterFlagCache();

        initRulesLoader(pt);

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        String igniteApiCaPem = pt.get("ignite.api.tls.ca.pem", null);
        this.igniteApiClient = new IgniteRulesApiClient(igniteApiUrl, igniteApiCaPem, jwt);

        boolean testCachesEnabled = pt.getBoolean("test.politic.caches.enabled", false);
        if (testCachesEnabled) {
            initTestCaches(pt);
        } else {
            log.info("[TEST] test caches initialization is disabled.");
        }

        boolean bootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);

        if (bootstrapEnabled) {
            CacheBootstrapService bootstrapService = new CacheBootstrapService(
                    igniteApiClient,
                    reloader,
                    rulesRegistry,
                    dataset2ControlAreaCache,
                    controlAreaRulesCache,
                    errorMessagesCache,
                    datasetExclusionCache,
                    filterFlagCache,
                    politicsBootstrapEnabled
            );
            bootstrapService.initializeAll();
        } else {
            log.info("[INIT] startup cache bootstrap is disabled. Waiting for Kafka cache update events.");
        }
    }

    private void initRulesLoader(ParameterTool pt) {
        String mode = pt.get("rules.loader", "http").toLowerCase(Locale.ROOT).trim();

        BytecodeSource rawSource;

        if ("http".equals(mode)) {
            String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
            String igniteApiCaPem = pt.get("ignite.api.tls.ca.pem", null);

            IgniteRulesApiClient apiClient = new IgniteRulesApiClient(igniteApiUrl, igniteApiCaPem, jwt);
            rawSource = new HttpBytecodeSource(apiClient);
            this.closeable = null;

            log.info("[RULES] loader=http apiUrl={}", igniteApiUrl);

        } else if ("thin".equals(mode)) {
            String igniteHost = pt.get("ignite.host", "127.0.0.1");
            int ignitePort = pt.getInt("ignite.port", 10800);

            IgniteClientFacadeImpl ignite = new IgniteClientFacadeImpl(igniteHost, ignitePort);
            IgniteClientFacade facade = ignite;
            rawSource = new IgniteBytecodeSource(facade);
            this.closeable = ignite;

            log.info("[RULES] loader=thin host={} port={}", igniteHost, ignitePort);

        } else {
            throw new IllegalArgumentException("Unknown rules.loader=" + mode + " (use thin|http)");
        }

        this.bytecodeSource = new TimedBytecodeSource(rawSource, log::info);
        this.reloader = new RulesReloader(bytecodeSource, rulesRegistry);

        log.info("[RULES] rules loader initialized. Actual cache versions will be loaded during startup bootstrap.");
    }

    public void reload(CacheUpdateEvent ev) {
        if (ev == null || !ev.isValid()) {
            return;
        }

        switch (ev.cacheName) {
            case CACHE_COMPILED_RULES -> reloadCompiledRules(ev.version);
            case CACHE_POLITICS -> reloadPoliticsCaches(ev.version);
            default -> throw new IllegalArgumentException("Unsupported cacheName=" + ev.cacheName);
        }
    }

    private void reloadCompiledRules(long version) {
        String fullCacheName = CACHE_COMPILED_RULES + "_" + version;

        log.info("[CACHE] Reloading compiled rules from {}", fullCacheName);

        reloader.reloadAllStrict(fullCacheName);

        log.info("[CACHE] compiled_rules reloaded: version={}, size={}",
                version, rulesRegistry.size());
    }

    private void reloadPoliticsCaches(long version) {
        String versionStr = String.valueOf(version);

        String dataset2ControlAreaName = CACHE_POLITICS_DATASET2CONTROL_AREA + "_" + version;
        String controlAreaRulesName = CACHE_POLITICS_CONTROL_AREA_RULES + "_" + version;
        String errorMessagesName = CACHE_POLITICS_ERROR_MESSAGES + "_" + version;
        String datasetExclusionName = CACHE_POLITICS_DATASET_EXCLUSION + "_" + version;
        String filterFlagName = CACHE_POLITICS_FILTER_FLAG + "_" + version;

        log.info("[CACHE] Reloading politics bundle for version={}", version);

        CacheResponseDto<String, Object> dataset2ControlAreaResponse =
                igniteApiClient.getVersionedCache(dataset2ControlAreaName);

        CacheResponseDto<String, Object> controlAreaRulesResponse =
                igniteApiClient.getVersionedCache(controlAreaRulesName);

        CacheResponseDto<String, Object> errorMessagesResponse =
                igniteApiClient.getVersionedCache(errorMessagesName);

        CacheResponseDto<String, Object> datasetExclusionResponse =
                igniteApiClient.getVersionedCache(datasetExclusionName);

        CacheResponseDto<String, Object> filterFlagResponse =
                igniteApiClient.getVersionedCache(filterFlagName);

        Map<String, String> dataset2ControlArea =
                cachePayloadConverter.toStringMap(dataset2ControlAreaResponse.getCache(), dataset2ControlAreaName);

        Map<String, Map<String, Set<String>>> controlAreaRules =
                cachePayloadConverter.toNestedRulesMap(controlAreaRulesResponse.getCache(), controlAreaRulesName);

        Map<String, Map<String, String>> errorMessages =
                cachePayloadConverter.toNestedStringMap(errorMessagesResponse.getCache(), errorMessagesName);

        Map<String, Set<String>> datasetExclusion =
                cachePayloadConverter.toSetMap(datasetExclusionResponse.getCache(), datasetExclusionName);

        Map<String, Boolean> filterFlags =
                cachePayloadConverter.toBooleanMap(filterFlagResponse.getCache(), filterFlagName);

        dataset2ControlAreaCache.replaceAll(dataset2ControlArea, versionStr);
        controlAreaRulesCache.replaceAll(controlAreaRules, versionStr);
        errorMessagesCache.replaceAll(errorMessages, versionStr);
        datasetExclusionCache.replaceAll(datasetExclusion, versionStr);
        filterFlagCache.replaceAll(filterFlags, versionStr);

        log.info(
                "[CACHE] politics reloaded: version={}, dataset2controlArea={}, controlAreaRules={}, errorMessages={}, datasetExclusion={}, filterFlag={}",
                version,
                dataset2ControlArea.size(),
                controlAreaRules.size(),
                errorMessages.size(),
                datasetExclusion.size(),
                filterFlags.size()
        );
    }

    private void initTestCaches(ParameterTool pt) {
        String path = pt.get("test.politic.caches.path", "").trim();

        if (path.isBlank()) {
            throw new IllegalArgumentException(
                    "test.caches.path must be provided for test caches initialization"
            );
        }

        try {
            com.gpb.datafirewall.dto.TestCachesConfigDto cfg =
                    mapper.readValue(
                            Files.readString(
                                    Path.of(path),
                                    StandardCharsets.UTF_8
                            ),
                            com.gpb.datafirewall.dto.TestCachesConfigDto.class
                    );

            String version = (cfg.getVersion() == null || cfg.getVersion().isBlank())
                    ? "test"
                    : cfg.getVersion().trim();

            rulesRegistry.replaceAll(Map.of(), version);

            dataset2ControlAreaCache.replaceAll(
                    cfg.getDataset2ControlArea() == null ? Map.of() : cfg.getDataset2ControlArea(),
                    version
            );

            Map<String, Map<String, Set<String>>> controlAreaRules = new LinkedHashMap<>();
            if (cfg.getControlAreaRules() != null) {
                for (Map.Entry<String, Map<String, List<String>>> areaEntry : cfg.getControlAreaRules().entrySet()) {
                    Map<String, Set<String>> fieldRules = new LinkedHashMap<>();
                    if (areaEntry.getValue() != null) {
                        for (Map.Entry<String, List<String>> fieldEntry : areaEntry.getValue().entrySet()) {
                            fieldRules.put(
                                    fieldEntry.getKey(),
                                    fieldEntry.getValue() == null
                                            ? Set.of()
                                            : new LinkedHashSet<>(fieldEntry.getValue())
                            );
                        }
                    }
                    controlAreaRules.put(areaEntry.getKey(), fieldRules);
                }
            }

            Map<String, Set<String>> datasetExclusion = new LinkedHashMap<>();
            if (cfg.getDatasetExclusion() != null) {
                for (Map.Entry<String, List<String>> entry : cfg.getDatasetExclusion().entrySet()) {
                    datasetExclusion.put(
                            entry.getKey(),
                            entry.getValue() == null
                                    ? Set.of()
                                    : new LinkedHashSet<>(entry.getValue())
                    );
                }
            }

            controlAreaRulesCache.replaceAll(controlAreaRules, version);
            errorMessagesCache.replaceAll(
                    cfg.getErrorMessages() == null ? Map.of() : cfg.getErrorMessages(),
                    version
            );
            datasetExclusionCache.replaceAll(datasetExclusion, version);
            filterFlagCache.replaceAll(
                    cfg.getFilterFlag() == null ? Map.of() : cfg.getFilterFlag(),
                    version
            );

            log.info(
                    "[TEST] caches initialized from file: path={}, version={}, rules={}, dataset2controlArea={}, controlAreaRules={}, errorMessages={}, datasetExclusion={}, filterFlag={}",
                    path,
                    version,
                    rulesRegistry.size(),
                    dataset2ControlAreaCache.size(),
                    controlAreaRulesCache.size(),
                    errorMessagesCache.size(),
                    datasetExclusionCache.size(),
                    filterFlagCache.size()
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test caches from file: " + path, e);
        }
    }

    public Map<String, Rule> rulesSnapshot() {
        return rulesRegistry.snapshot();
    }

    public int rulesSize() {
        return rulesRegistry == null ? 0 : rulesRegistry.size();
    }

    public String controlAreaByDataset(String datasetCode) {
        return dataset2ControlAreaCache.get(datasetCode);
    }

    public Map<String, Set<String>> fieldToRules(String controlArea) {
        return controlAreaRulesCache.get(controlArea);
    }

    public Map<String, Map<String, String>> errorMessagesSnapshot() {
        Map<String, Map<String, String>> snapshot = errorMessagesCache.snapshot();
        return snapshot == null || snapshot.isEmpty() ? Map.of() : snapshot;
    }

    public Set<String> excludedBlocks(String controlArea) {
        Set<String> result = datasetExclusionCache.get(controlArea);
        return result == null ? Set.of() : result;
    }

    public Boolean filterFlag(String controlArea) {
        return filterFlagCache.get(controlArea);
    }

    public int dataset2ControlAreaSize() {
        return dataset2ControlAreaCache == null ? 0 : dataset2ControlAreaCache.size();
    }

    public int controlAreaRulesSize() {
        return controlAreaRulesCache == null ? 0 : controlAreaRulesCache.size();
    }

    public int errorMessagesSize() {
        return errorMessagesCache == null ? 0 : errorMessagesCache.size();
    }

    public int datasetExclusionSize() {
        return datasetExclusionCache == null ? 0 : datasetExclusionCache.size();
    }

    public int filterFlagSize() {
        return filterFlagCache == null ? 0 : filterFlagCache.size();
    }

    @Override
    public void close() {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close cache runtime resources", e);
        }
    }
}
