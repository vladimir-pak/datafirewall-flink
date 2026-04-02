package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.model.Rule;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.cache.CompiledRulesRegistry;
import ru.gpbapp.datafirewallflink.cache.PoliticsControlAreaRulesCache;
import ru.gpbapp.datafirewallflink.cache.PoliticsDatasetControlAreaCache;
import ru.gpbapp.datafirewallflink.cache.PoliticsDatasetExclusionCache;
import ru.gpbapp.datafirewallflink.cache.PoliticsErrorMessagesCache;
import ru.gpbapp.datafirewallflink.cache.PoliticsFilterFlagCache;
import ru.gpbapp.datafirewallflink.config.IgniteRulesApiClient;
import ru.gpbapp.datafirewallflink.converter.MappingNormalizer;
import ru.gpbapp.datafirewallflink.dto.CacheResponseDto;
import ru.gpbapp.datafirewallflink.dto.HttpBytecodeSource;
import ru.gpbapp.datafirewallflink.dto.IgniteBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteClientFacadeImpl;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEvent;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.rule.RulesReloader;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class MqWithRulesReloadBroadcastProcessFunction
        extends BroadcastProcessFunction<MqRecord, CacheUpdateEvent, MqReply> {

    private static final Logger log =
            LoggerFactory.getLogger(MqWithRulesReloadBroadcastProcessFunction.class);

    private static final String CACHE_COMPILED_RULES = "compiled_rules";
    private static final String CACHE_POLITICS = "politics";

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

    private final MapStateDescriptor<String, CacheUpdateEvent> rulesBroadcastDesc;

    private transient ObjectMapper mapper;

    private transient CompiledRulesRegistry rulesRegistry;
    private transient RulesReloader reloader;
    private transient BytecodeSource bytecodeSource;
    private transient AutoCloseable closeable;
    private transient IgniteRulesApiClient igniteApiClient;

    private transient PoliticsDatasetControlAreaCache politicsDataset2ControlAreaCache;
    private transient PoliticsControlAreaRulesCache politicsControlAreaRulesCache;
    private transient PoliticsErrorMessagesCache politicsErrorMessagesCache;
    private transient PoliticsDatasetExclusionCache politicsDatasetExclusionCache;
    private transient PoliticsFilterFlagCache politicsFilterFlagCache;

    private transient ValidationService validationService;
    private transient ShortAnswerService shortAnswerService;
    private transient DetailAnswerService detailAnswerService;
    private transient MappingNormalizer normalizer;

    private transient boolean logPayloads;
    private transient int logPreviewLen;

    public MqWithRulesReloadBroadcastProcessFunction(
            MapStateDescriptor<String, CacheUpdateEvent> rulesBroadcastDesc
    ) {
        this.rulesBroadcastDesc = rulesBroadcastDesc;
    }

    @Override
    public void open(Configuration parameters) {
        RuntimeContext rc = getRuntimeContext();

        ParameterTool pt = (ParameterTool) rc.getExecutionConfig().getGlobalJobParameters();
        if (pt == null) {
            pt = ParameterTool.fromMap(Map.of());
        }

        this.logPayloads = pt.getBoolean("log.payloads", false);
        this.logPreviewLen = pt.getInt("log.preview.len", 600);

        this.mapper = new ObjectMapper();

        this.rulesRegistry = new CompiledRulesRegistry();
        this.politicsDataset2ControlAreaCache = new PoliticsDatasetControlAreaCache();
        this.politicsControlAreaRulesCache = new PoliticsControlAreaRulesCache();
        this.politicsErrorMessagesCache = new PoliticsErrorMessagesCache();
        this.politicsDatasetExclusionCache = new PoliticsDatasetExclusionCache();
        this.politicsFilterFlagCache = new PoliticsFilterFlagCache();

        initRulesLoaderAndLoad(pt);

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        this.igniteApiClient = new IgniteRulesApiClient(igniteApiUrl);

        boolean bootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);

        boolean testCachesEnabled = pt.getBoolean("test.politic.caches.enabled", false);
        if (testCachesEnabled) {
            initTestCaches(pt);
        } else {
            log.info("[TEST] test caches initialization is disabled.");
        }

        if (bootstrapEnabled) {
            CacheBootstrapService bootstrapService = new CacheBootstrapService(
                    igniteApiClient,
                    reloader,
                    rulesRegistry,
                    politicsDataset2ControlAreaCache,
                    politicsControlAreaRulesCache,
                    politicsErrorMessagesCache,
                    politicsDatasetExclusionCache,
                    politicsFilterFlagCache,
                    politicsBootstrapEnabled
            );
            bootstrapService.initializeAll();
        } else {
            log.info("[INIT] startup cache bootstrap is disabled. Waiting for Kafka cache update events.");
        }

        this.validationService = new ValidationService();
        this.shortAnswerService = new ShortAnswerService(mapper);
        this.detailAnswerService = new DetailAnswerService(mapper);
        this.normalizer = new MappingNormalizer(mapper);

        log.info(
                "[INIT] subtask={} log.payloads={} rulesLoaded={} dataset2controlAreaLoaded={} controlAreaRulesLoaded={} errorMessagesLoaded={} datasetExclusionLoaded={} filterFlagLoaded={}",
                rc.getIndexOfThisSubtask(),
                logPayloads,
                rulesRegistry.size(),
                politicsDataset2ControlAreaCache.size(),
                politicsControlAreaRulesCache.size(),
                politicsErrorMessagesCache.size(),
                politicsDatasetExclusionCache.size(),
                politicsFilterFlagCache.size()
        );
    }

    private void initRulesLoaderAndLoad(ParameterTool pt) {
        String mode = pt.get("rules.loader", "http").toLowerCase(Locale.ROOT).trim();

        BytecodeSource rawSource;

        if ("http".equals(mode)) {
            String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
            IgniteRulesApiClient apiClient = new IgniteRulesApiClient(igniteApiUrl);
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

    @Override
    public void processElement(MqRecord in, ReadOnlyContext ctx, Collector<MqReply> out) {
        if (in == null || in.payload == null || in.payload.isBlank()) {
            log.warn("[PIPE][no-qid] Empty MQ payload");
            return;
        }

        String raw = in.payload;

        try {
            JsonNode originalEvent = mapper.readTree(raw);
            String qid = originalEvent.path("dfw_query_id").asText("no-qid");

            ReadOnlyBroadcastState<String, CacheUpdateEvent> st =
                    ctx.getBroadcastState(rulesBroadcastDesc);

            CacheUpdateEvent compiledRulesEvent =
                    st != null ? st.get(CACHE_COMPILED_RULES) : null;
            CacheUpdateEvent politicsEvent =
                    st != null ? st.get(CACHE_POLITICS) : null;

            Long compiledRulesVersion = compiledRulesEvent != null
                    ? compiledRulesEvent.version
                    : null;
            Long politicsVersion = politicsEvent != null
                    ? politicsEvent.version
                    : null;

            log.info(
                    "[PIPE][{}] using compiledRulesVersion={} politicsVersion={} rulesCount={}",
                    qid,
                    compiledRulesVersion,
                    politicsVersion,
                    rulesRegistry.size()
            );

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 1) MQ_IN:\n{}", qid, maskJsonPretty(raw));
            }

            String datasetCode = extractFirstDatasetCode(originalEvent);
            if (datasetCode == null || datasetCode.isBlank()) {
                log.warn("[PIPE][{}] dataset_code not found in input payload", qid);
                return;
            }

            String controlArea = politicsDataset2ControlAreaCache.get(datasetCode);
            if (controlArea == null || controlArea.isBlank()) {
                log.warn("[PIPE][{}] controlArea not found for datasetCode={}", qid, datasetCode);
                return;
            }

            Map<String, Set<String>> allFieldToRules = politicsControlAreaRulesCache.get(controlArea);
            if (allFieldToRules == null || allFieldToRules.isEmpty()) {
                log.warn("[PIPE][{}] fieldToRules not found for controlArea={} datasetCode={}",
                        qid, controlArea, datasetCode);
                return;
            }

            log.info("[PIPE][{}] datasetCode={} controlArea={} mappedFields={}",
                    qid, datasetCode, controlArea, allFieldToRules.keySet());

            Map<String, String> normalizedMap = normalizer.normalize(originalEvent);

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 2) NORMALIZED_MAP size={} keys={}",
                        qid, normalizedMap.size(), normalizedMap.keySet());
                log.info("[PIPE][{}] 2) NORMALIZED_MAP full(masked):\n{}",
                        qid,
                        prettyObject(maskMap(normalizedMap)));
            }

            Map<String, Rule> compiledRules = rulesRegistry.snapshot();

            // блоки, которые надо валидировать отдельно
            Set<String> excludedBlocks = politicsDatasetExclusionCache.get(controlArea);
            if (excludedBlocks == null) {
                excludedBlocks = Set.of();
            }

            JsonNode dataNode = originalEvent.path("data");

            Map<String, JsonNode> blockNodes = collectTopLevelBlockNodes(dataNode);
            Map<String, Map<String, String>> excludedBlockNormalizedMaps = new LinkedHashMap<>();
            Map<String, String> excludedBlockDatasetCodes = new LinkedHashMap<>();
            Set<String> excludedLogicalFields = new LinkedHashSet<>();

            for (String blockName : excludedBlocks) {
                JsonNode blockNode = blockNodes.get(blockName);
                if (blockNode == null || !blockNode.isObject()) {
                    continue;
                }

                Map<String, String> blockNormalized = normalizeSingleBlock(blockName, blockNode);
                excludedBlockNormalizedMaps.put(blockName, blockNormalized);
                excludedLogicalFields.addAll(blockNormalized.keySet());

                String blockDatasetCode = text(blockNode, "dataset_code", blockName);
                excludedBlockDatasetCodes.put(blockName, blockDatasetCode);

                if (logPayloads && log.isInfoEnabled()) {
                    log.info("[PIPE][{}] 2) BLOCK_NORMALIZED_MAP block={} datasetCode={} full(masked):\n{}",
                            qid,
                            blockName,
                            blockDatasetCode,
                            prettyObject(maskMap(blockNormalized)));
                }
            }

            // основной dataset без полей excluded blocks
            Map<String, String> mainNormalizedMap =
                    removeLogicalFields(normalizedMap, excludedLogicalFields);

            Map<String, Set<String>> mainFieldToRules =
                    removeLogicalFieldsFromFieldToRules(allFieldToRules, excludedLogicalFields);

            Map<String, String> mainEffectiveNormalizedMap =
                    buildEffectiveNormalizedMap(controlArea, mainNormalizedMap, mainFieldToRules);

            Map<String, Set<String>> mainEffectiveFieldToRules =
                    buildEffectiveFieldToRules(controlArea, mainEffectiveNormalizedMap, mainFieldToRules);

            ValidationResult mainValidation = validationService.validate(
                    compiledRules,
                    mainEffectiveNormalizedMap,
                    mainEffectiveFieldToRules
            );

            Map<String, Map<String, String>> mergedDetailByField = new LinkedHashMap<>();
            if (mainValidation.detailByField() != null) {
                mergedDetailByField.putAll(mainValidation.detailByField());
            }

            Map<String, Map<String, Map<String, String>>> mergedDetailByDataset = new LinkedHashMap<>();
            mergedDetailByDataset.put(datasetCode, safeFieldMap(mainValidation.detailByField()));

            boolean anyError = "ERROR".equalsIgnoreCase(mainValidation.allResult());
            boolean anyRuleException = "RULE_EXCEPTION".equalsIgnoreCase(mainValidation.processStatus());

            // отдельная валидация excluded blocks
            for (String blockName : excludedBlocks) {
                JsonNode blockNode = blockNodes.get(blockName);
                if (blockNode == null || !blockNode.isObject()) {
                    continue;
                }

                String blockDatasetCode = excludedBlockDatasetCodes.getOrDefault(blockName, blockName);
                String blockControlArea = politicsDataset2ControlAreaCache.get(blockDatasetCode);
                if (blockControlArea == null || blockControlArea.isBlank()) {
                    blockControlArea = controlArea;
                }

                Map<String, String> blockNormalizedMap =
                        excludedBlockNormalizedMaps.getOrDefault(blockName, Map.of());

                Set<String> blockLogicalFields = collectLogicalFieldsFromBlock(blockNode);

                // если mapping.* в блоке пустой/неполный — подстрахуемся реально найденными logical fields
                blockLogicalFields.addAll(blockNormalizedMap.keySet());

                Map<String, Set<String>> blockFieldToRules =
                        selectFieldToRulesByKeys(allFieldToRules, blockLogicalFields);

                Map<String, String> blockEffectiveNormalizedMap =
                        buildEffectiveNormalizedMap(blockControlArea, blockNormalizedMap, blockFieldToRules);

                Map<String, Set<String>> blockEffectiveFieldToRules =
                        buildEffectiveFieldToRules(blockControlArea, blockEffectiveNormalizedMap, blockFieldToRules);

                ValidationResult blockValidation = validationService.validate(
                        compiledRules,
                        blockEffectiveNormalizedMap,
                        blockEffectiveFieldToRules
                );

                if (blockValidation.detailByField() != null) {
                    mergedDetailByField.putAll(blockValidation.detailByField());
                }
                mergedDetailByDataset.put(blockDatasetCode, safeFieldMap(blockValidation.detailByField()));

                if ("ERROR".equalsIgnoreCase(blockValidation.allResult())) {
                    anyError = true;
                }
                if ("RULE_EXCEPTION".equalsIgnoreCase(blockValidation.processStatus())) {
                    anyRuleException = true;
                }
            }

            ValidationResult finalValidation = new ValidationResult(
                    null,
                    anyError ? "ERROR" : "SUCCESS",
                    anyRuleException ? "RULE_EXCEPTION" : "OK",
                    Map.copyOf(mergedDetailByField),
                    Map.copyOf(mergedDetailByDataset)
            );

            String shortJson = shortAnswerService.build(originalEvent, finalValidation);
            if (shortJson == null) {
                log.warn("[PIPE][{}] ShortAnswerService returned null.", qid);
                return;
            }

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 3) ANSWER_SHORT:\n{}", qid, maskJsonPretty(shortJson));
            }

            String detailJson = detailAnswerService.build(originalEvent, finalValidation);
            if (detailJson != null) {
                if (logPayloads && log.isInfoEnabled()) {
                    log.info("[PIPE][{}] 4) ANSWER_DETAIL:\n{}", qid, maskJsonPretty(detailJson));
                }
            } else {
                log.warn("[PIPE][{}] DetailAnswerService returned null.", qid);
            }

            out.collect(new MqReply(in.msgId, shortJson));

        } catch (Exception e) {
            log.error("Failed to build answers.", e);
        }
    }

    @Override
    public void processBroadcastElement(
            CacheUpdateEvent ev,
            Context ctx,
            Collector<MqReply> out
    ) throws Exception {
        if (ev == null || !ev.isValid()) {
            return;
        }

        BroadcastState<String, CacheUpdateEvent> st =
                ctx.getBroadcastState(rulesBroadcastDesc);

        CacheUpdateEvent current = st.get(ev.cacheName);

        if (current != null && ev.version <= current.version) {
            log.info("[CACHE][KAFKA] ignore cacheName={} version={} (current={})",
                    ev.cacheName, ev.version, current.version);
            return;
        }

        log.info("[CACHE][KAFKA] new event cacheName={} version={} (prev={}) -> reloading...",
                ev.cacheName,
                ev.version,
                current != null ? current.version : null);

        long t0 = System.nanoTime();
        try {
            switch (ev.cacheName) {
                case CACHE_COMPILED_RULES -> reloadCompiledRules(ev.version);
                case CACHE_POLITICS -> reloadPoliticsCaches(ev.version);
                default -> {
                    log.warn("[CACHE][KAFKA] unsupported cacheName={}", ev.cacheName);
                    return;
                }
            }

            long ms = (System.nanoTime() - t0) / 1_000_000;
            st.put(ev.cacheName, ev);

            log.info("[CACHE][KAFKA] reload OK cacheName={} version={} in {}ms",
                    ev.cacheName, ev.version, ms);

        } catch (Exception ex) {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.error("[CACHE][KAFKA] reload FAILED cacheName={} version={} after {}ms (keep old snapshot)",
                    ev.cacheName, ev.version, ms, ex);
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
                toStringMap(dataset2ControlAreaResponse.getCache(), dataset2ControlAreaName);

        Map<String, Map<String, Set<String>>> controlAreaRules =
                toNestedRulesMap(controlAreaRulesResponse.getCache(), controlAreaRulesName);

        Map<String, String> errorMessages =
                toStringMap(errorMessagesResponse.getCache(), errorMessagesName);

        Map<String, Set<String>> datasetExclusion =
                toSetMap(datasetExclusionResponse.getCache(), datasetExclusionName);

        Map<String, Boolean> filterFlags =
                toBooleanMap(filterFlagResponse.getCache(), filterFlagName);

        politicsDataset2ControlAreaCache.replaceAll(dataset2ControlArea, versionStr);
        politicsControlAreaRulesCache.replaceAll(controlAreaRules, versionStr);
        politicsErrorMessagesCache.replaceAll(errorMessages, versionStr);
        politicsDatasetExclusionCache.replaceAll(datasetExclusion, versionStr);
        politicsFilterFlagCache.replaceAll(filterFlags, versionStr);

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

    private String extractFirstDatasetCode(JsonNode originalEvent) {
        JsonNode dataNode = originalEvent.path("data");
        if (!dataNode.isObject()) {
            return null;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            JsonNode child = entry.getValue();
            if (child != null && child.isObject()) {
                JsonNode datasetCodeNode = child.get("dataset_code");
                if (datasetCodeNode != null && !datasetCodeNode.isNull()) {
                    String datasetCode = datasetCodeNode.asText(null);
                    if (datasetCode != null && !datasetCode.isBlank()) {
                        return datasetCode.trim();
                    }
                }
            }
        }

        return null;
    }

    private Map<String, JsonNode> collectTopLevelBlockNodes(JsonNode dataNode) {
        Map<String, JsonNode> result = new LinkedHashMap<>();
        if (dataNode == null || !dataNode.isObject()) {
            return result;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            if (entry.getValue() != null && entry.getValue().isObject()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private Map<String, String> normalizeSingleBlock(String blockName, JsonNode blockNode) {
        com.fasterxml.jackson.databind.node.ObjectNode root = mapper.createObjectNode();
        com.fasterxml.jackson.databind.node.ObjectNode data = mapper.createObjectNode();
        data.set(blockName, blockNode);
        root.set("data", data);
        return normalizer.normalize(root);
    }

    private Set<String> collectLogicalFieldsFromBlock(JsonNode blockNode) {
        Set<String> out = new LinkedHashSet<>();
        if (blockNode == null || !blockNode.isObject()) {
            return out;
        }

        Iterator<Map.Entry<String, JsonNode>> it = blockNode.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            JsonNode value = e.getValue();

            if (key == null || !key.startsWith("mapping.") || value == null || value.isNull()) {
                continue;
            }

            String logical = value.asText(null);
            if (logical == null || logical.isBlank() || "none".equalsIgnoreCase(logical)) {
                continue;
            }

            out.add(logical.trim());
        }
        return out;
    }

    private Map<String, String> removeLogicalFields(
            Map<String, String> source,
            Set<String> toRemove
    ) {
        if (source == null || source.isEmpty()) {
            return new LinkedHashMap<>();
        }
        if (toRemove == null || toRemove.isEmpty()) {
            return new LinkedHashMap<>(source);
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            if (!toRemove.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private Map<String, Set<String>> removeLogicalFieldsFromFieldToRules(
            Map<String, Set<String>> fieldToRules,
            Set<String> toRemove
    ) {
        if (fieldToRules == null || fieldToRules.isEmpty()) {
            return new LinkedHashMap<>();
        }
        if (toRemove == null || toRemove.isEmpty()) {
            return new LinkedHashMap<>(fieldToRules);
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : fieldToRules.entrySet()) {
            if (!toRemove.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private Map<String, Set<String>> selectFieldToRulesByKeys(
            Map<String, Set<String>> fieldToRules,
            Set<String> allowedFields
    ) {
        Map<String, Set<String>> result = new LinkedHashMap<>();
        if (fieldToRules == null || fieldToRules.isEmpty() || allowedFields == null || allowedFields.isEmpty()) {
            return result;
        }

        for (Map.Entry<String, Set<String>> entry : fieldToRules.entrySet()) {
            if (allowedFields.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private Map<String, Set<String>> filterFieldToRulesByNormalizedMap(
            Map<String, Set<String>> fieldToRules,
            Map<String, String> normalizedMap
    ) {
        Map<String, Set<String>> result = new LinkedHashMap<>();
        if (fieldToRules == null || fieldToRules.isEmpty() || normalizedMap == null || normalizedMap.isEmpty()) {
            return result;
        }

        for (Map.Entry<String, Set<String>> entry : fieldToRules.entrySet()) {
            String logicalField = entry.getKey();
            if (logicalField == null || logicalField.isBlank()) {
                continue;
            }

            if (normalizedMap.containsKey(logicalField)) {
                result.put(logicalField, entry.getValue());
                continue;
            }

            String alt = logicalField.replace('.', ',');
            if (normalizedMap.containsKey(alt)) {
                result.put(logicalField, entry.getValue());
            }
        }

        return result;
    }

    private Map<String, String> buildEffectiveNormalizedMap(
            String controlArea,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRules
    ) {
        Map<String, String> safeNormalized = normalizedMap == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(normalizedMap);

        Boolean filterFlag = politicsFilterFlagCache.get(controlArea);

        // false/null -> текущая логика
        if (!Boolean.TRUE.equals(filterFlag)) {
            return safeNormalized;
        }

        // true -> добавляем все поля из fieldToRules, отсутствующие = null
        Map<String, String> effective = new LinkedHashMap<>();

        for (String logicalField : fieldToRules.keySet()) {
            if (logicalField == null || logicalField.isBlank()) {
                continue;
            }

            if (safeNormalized.containsKey(logicalField)) {
                effective.put(logicalField, safeNormalized.get(logicalField));
                continue;
            }

            String alt = logicalField.replace('.', ',');
            if (safeNormalized.containsKey(alt)) {
                effective.put(logicalField, safeNormalized.get(alt));
                continue;
            }

            effective.put(logicalField, null);
        }

        // на всякий случай оставим и прочие фактически пришедшие поля
        for (Map.Entry<String, String> entry : safeNormalized.entrySet()) {
            effective.putIfAbsent(entry.getKey(), entry.getValue());
        }

        return effective;
    }

    private Map<String, Set<String>> buildEffectiveFieldToRules(
            String controlArea,
            Map<String, String> effectiveNormalizedMap,
            Map<String, Set<String>> fieldToRules
    ) {
        Boolean filterFlag = politicsFilterFlagCache.get(controlArea);

        if (Boolean.TRUE.equals(filterFlag)) {
            return new LinkedHashMap<>(fieldToRules);
        }

        return filterFieldToRulesByNormalizedMap(fieldToRules, effectiveNormalizedMap);
    }

    private Map<String, Map<String, String>> safeFieldMap(Map<String, Map<String, String>> source) {
        return source == null ? Map.of() : source;
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

    private Map<String, Boolean> toBooleanMap(Map<String, Object> payload, String cacheName) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

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

    private Map<String, Set<String>> toSetMap(Map<String, Object> payload, String cacheName) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }

            Set<String> value = mapper.convertValue(
                    entry.getValue(),
                    mapper.getTypeFactory().constructCollectionType(Set.class, String.class)
            );

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

        var typeFactory = mapper.getTypeFactory();
        var setType = typeFactory.constructCollectionType(Set.class, String.class);
        var innerMapType = typeFactory.constructMapType(
                LinkedHashMap.class,
                typeFactory.constructType(String.class),
                setType
        );

        Map<String, Map<String, Set<String>>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }

            Map<String, Set<String>> value = mapper.convertValue(
                    entry.getValue(),
                    innerMapType
            );

            result.put(entry.getKey(), value);
        }
        return result;
    }

    @Override
    public void close() {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close resources", e);
        }
    }

    private String prettyObject(Object o) {
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } catch (Exception e) {
            return String.valueOf(o);
        }
    }

    private String maskInline(String s) {
        if (s == null) {
            return null;
        }
        return s
                .replaceAll("(\"birthdate\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"clientSnils\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"snils\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"number\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"series\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"departmentCode\"\\s*:\\s*\")[^\"]*(\")", "$1***$2");
    }

    private String maskJsonPretty(String json) {
        if (json == null || json.isBlank()) {
            return json;
        }
        try {
            JsonNode root = mapper.readTree(json);
            maskNode(root);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            return maskInline(json);
        }
    }

    private void maskNode(JsonNode node) {
        if (node == null) {
            return;
        }
        if (node.isObject()) {
            Iterator<String> it = node.fieldNames();
            while (it.hasNext()) {
                String fn = it.next();
                JsonNode child = node.get(fn);
                if (isSensitiveKey(fn)
                        && node instanceof com.fasterxml.jackson.databind.node.ObjectNode obj) {
                    obj.put(fn, "***");
                } else {
                    maskNode(child);
                }
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                maskNode(child);
            }
        }
    }

    private boolean isSensitiveKey(String key) {
        if (key == null) {
            return false;
        }
        String k = key.toLowerCase(Locale.ROOT);
        return k.equals("birthdate")
                || k.equals("clientsnils")
                || k.equals("snils")
                || k.equals("inn")
                || k.equals("number")
                || k.equals("series")
                || k.equals("departmentcode");
    }

    private Map<String, String> maskMap(Map<String, String> m) {
        if (m == null) {
            return Map.of();
        }

        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();

            if (k != null && (isSensitiveKey(k)
                    || k.toLowerCase(Locale.ROOT).contains("snils")
                    || k.toLowerCase(Locale.ROOT).contains("birthdate")
                    || k.toLowerCase(Locale.ROOT).contains("passport")
                    || k.toLowerCase(Locale.ROOT).contains("number"))) {
                out.put(k, "***");
            } else {
                out.put(k, v);
            }
        }
        return out;
    }

    private String text(JsonNode node, String field, String def) {
        if (node == null) {
            return def;
        }
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? def : v.asText(def);
    }


    private void initTestCaches(ParameterTool pt) {
        String path = pt.get("test.politic.caches.path", "").trim();

        if (path.isBlank()) {
            throw new IllegalArgumentException(
                    "test.caches.path must be provided for test caches initialization"
            );
        }

        try {
            ru.gpbapp.datafirewallflink.dto.TestCachesConfigDto cfg =
                    mapper.readValue(
                            java.nio.file.Files.readString(
                                    java.nio.file.Path.of(path),
                                    java.nio.charset.StandardCharsets.UTF_8
                            ),
                            ru.gpbapp.datafirewallflink.dto.TestCachesConfigDto.class
                    );

            String version = (cfg.getVersion() == null || cfg.getVersion().isBlank())
                    ? "test"
                    : cfg.getVersion().trim();

            rulesRegistry.replaceAll(Map.of(), version);

            politicsDataset2ControlAreaCache.replaceAll(
                    cfg.getDataset2ControlArea() == null ? Map.of() : cfg.getDataset2ControlArea(),
                    version
            );

            Map<String, Map<String, Set<String>>> controlAreaRules = new LinkedHashMap<>();
            if (cfg.getControlAreaRules() != null) {
                for (Map.Entry<String, Map<String, java.util.List<String>>> areaEntry : cfg.getControlAreaRules().entrySet()) {
                    Map<String, Set<String>> fieldRules = new LinkedHashMap<>();
                    if (areaEntry.getValue() != null) {
                        for (Map.Entry<String, java.util.List<String>> fieldEntry : areaEntry.getValue().entrySet()) {
                            fieldRules.put(
                                    fieldEntry.getKey(),
                                    fieldEntry.getValue() == null
                                            ? java.util.Set.of()
                                            : new java.util.LinkedHashSet<>(fieldEntry.getValue())
                            );
                        }
                    }
                    controlAreaRules.put(areaEntry.getKey(), fieldRules);
                }
            }

            Map<String, Set<String>> datasetExclusion = new LinkedHashMap<>();
            if (cfg.getDatasetExclusion() != null) {
                for (Map.Entry<String, java.util.List<String>> entry : cfg.getDatasetExclusion().entrySet()) {
                    datasetExclusion.put(
                            entry.getKey(),
                            entry.getValue() == null
                                    ? java.util.Set.of()
                                    : new java.util.LinkedHashSet<>(entry.getValue())
                    );
                }
            }

            politicsControlAreaRulesCache.replaceAll(controlAreaRules, version);
            politicsErrorMessagesCache.replaceAll(
                    cfg.getErrorMessages() == null ? Map.of() : cfg.getErrorMessages(),
                    version
            );
            politicsDatasetExclusionCache.replaceAll(datasetExclusion, version);
            politicsFilterFlagCache.replaceAll(
                    cfg.getFilterFlag() == null ? Map.of() : cfg.getFilterFlag(),
                    version
            );

            log.info(
                    "[TEST] caches initialized from file: path={}, version={}, rules={}, dataset2controlArea={}, controlAreaRules={}, errorMessages={}, datasetExclusion={}, filterFlag={}",
                    path,
                    version,
                    rulesRegistry.size(),
                    politicsDataset2ControlAreaCache.size(),
                    politicsControlAreaRulesCache.size(),
                    politicsErrorMessagesCache.size(),
                    politicsDatasetExclusionCache.size(),
                    politicsFilterFlagCache.size()
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test caches from file: " + path, e);
        }
    }

}