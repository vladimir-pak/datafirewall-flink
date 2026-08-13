package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.model.Rule;
import com.gpb.datafirewall.validation.ValidationResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class MessageProcessingService {

    private static final Logger log =
            LoggerFactory.getLogger(MessageProcessingService.class);

    private static final String RESULT_ERROR = "ERROR";
    private static final String RESULT_WARNING = "WARNING";
    private static final String RESULT_SUCCESS = "SUCCESS";
    private static final String PROCESS_RULE_EXCEPTION = "RULE_EXCEPTION";
    private static final String PROCESS_OK = "OK";

    // Временная диагностика расхождений Flink vs old .NET.
    // После завершения анализа эти логи можно удалить.
    private static final Set<String> DIAG_RULE_IDS = Set.of(
            "1080",
            "1093",
            "10329",
            "10377",
            "10378",
            "10383",
            "1194"
    );

    private static final Set<String> DIAG_LOGICAL_FIELDS = Set.of(
            "ИНН.Номер свидетельства",
            "ОСНОВНЫЕ СВЕДЕНИЯ.СНИЛС",
            "КОНТАКТ.Почта.Электронный адрес (email)",
            "АДРЕС.Район",
            "АДРЕС.Строение"
    );

    private final ObjectMapper mapper;
    private final RulesCacheRuntime cacheRuntime;
    private final ValidationService validationService;
    private final ShortAnswerService shortAnswerService;
    private final DetailAnswerService detailAnswerService;
    private final MappingNormalizer normalizer;
    private final boolean logPayloads;

    public MessageProcessingService(
            ObjectMapper mapper,
            RulesCacheRuntime cacheRuntime,
            boolean logPayloads
    ) {
        this.mapper = mapper;
        this.cacheRuntime = cacheRuntime;
        this.logPayloads = logPayloads;
        this.validationService = new ValidationService();
        this.shortAnswerService = new ShortAnswerService(mapper);
        this.detailAnswerService = new DetailAnswerService(mapper);
        this.normalizer = new MappingNormalizer(mapper);
    }

    public ProcessingResult process(MessageRecord in) {
        if (in == null || in.payload == null || in.payload.isBlank()) {
            log.warn("[PIPE][no-qid] Empty input payload");
            return null;
        }

        String raw = in.payload;
        String eventId = extractEventId(in);

        try {
            JsonNode originalEvent = mapper.readTree(raw);

            logNullLikeInputDiagnostics(originalEvent, eventId);

            int nullLikeValuesConverted =
                    normalizeEmptyStringsToNull(originalEvent);

            log.info(
                    "[DIAG][eventId={}] normalizeEmptyStringsToNull convertedCount={}",
                    eventId,
                    nullLikeValuesConverted
            );

            String qid = originalEvent.path("dfw_query_id").asText(null);
            if (qid == null || qid.isBlank()) {
                qid = eventId;
            }

            log.info(
                    "[PIPE][{}][eventId={}] handler=flink",
                    qid,
                    eventId
            );

            if (logPayloads && log.isInfoEnabled()) {
                log.info(
                        "[PIPE][{}][eventId={}] 1) INBOUND:\n{}",
                        qid,
                        eventId,
                        maskJsonPretty(raw)
                );
            }

            String datasetCode = extractFirstDatasetCode(originalEvent);
            if (datasetCode == null || datasetCode.isBlank()) {
                log.warn(
                        "[PIPE][{}][eventId={}] dataset_code not found in input payload",
                        qid,
                        eventId
                );
                return null;
            }

            String controlArea =
                    cacheRuntime.controlAreaByDataset(datasetCode);

            log.info(
                    "[DIAG][eventId={}] datasetCode={} controlArea={}",
                    eventId,
                    datasetCode,
                    controlArea
            );

            if (controlArea == null || controlArea.isBlank()) {
                log.warn(
                        "[PIPE][{}][eventId={}] controlArea not found for datasetCode={}",
                        qid,
                        eventId,
                        datasetCode
                );
                return null;
            }

            Map<String, Set<String>> allFieldToRules =
                    cacheRuntime.fieldToRules(controlArea);

            if (allFieldToRules == null || allFieldToRules.isEmpty()) {
                log.warn(
                        "[PIPE][{}][eventId={}] fieldToRules not found for controlArea={} datasetCode={}",
                        qid,
                        eventId,
                        controlArea,
                        datasetCode
                );
                return null;
            }

            Boolean filterFlag =
                    cacheRuntime.filterFlag(controlArea);

            log.info(
                    "[DIAG][eventId={}] controlArea={} filterFlag={} fieldToRules.fields={} fieldToRules.ruleRefs={}",
                    eventId,
                    controlArea,
                    filterFlag,
                    allFieldToRules.size(),
                    countRuleReferences(allFieldToRules)
            );

            Map<String, String> normalizedMap =
                    normalizer.normalize(originalEvent);

            Map<String, Map<String, String>> errorMessagesByRule =
                    cacheRuntime.errorMessagesSnapshot();

            logDiagnosticFields(
                    "NORMALIZED",
                    eventId,
                    normalizedMap,
                    allFieldToRules
            );

            if (logPayloads && log.isInfoEnabled()) {
                log.info(
                        "[PIPE][{}] 2) NORMALIZED_MAP size={} keys={}",
                        qid,
                        normalizedMap.size(),
                        normalizedMap.keySet()
                );

                log.info(
                        "[PIPE][{}] 2) NORMALIZED_MAP full(masked):\n{}",
                        qid,
                        prettyObject(maskMap(normalizedMap))
                );
            }

            Map<String, Rule> compiledRules =
                    cacheRuntime.rulesSnapshot();

            log.info(
                    "[DIAG][eventId={}] compiledRules.size={} errorMessages.size={} diagnosticRules={}",
                    eventId,
                    compiledRules == null ? 0 : compiledRules.size(),
                    errorMessagesByRule == null ? 0 : errorMessagesByRule.size(),
                    diagnosticRulePresence(compiledRules)
            );

            logMissingCompiledRules(
                    eventId,
                    allFieldToRules,
                    compiledRules
            );

            Set<String> excludedBlocks =
                    cacheRuntime.excludedBlocks(controlArea);

            log.info(
                    "[DIAG][eventId={}] excludedBlocks={}",
                    eventId,
                    excludedBlocks
            );

            JsonNode dataNode =
                    originalEvent.path("data");

            Map<String, JsonNode> blockNodes =
                    collectTopLevelBlockNodes(dataNode);

            Map<String, Map<String, String>> excludedBlockNormalizedMaps =
                    new LinkedHashMap<>();

            Map<String, String> excludedBlockDatasetCodes =
                    new LinkedHashMap<>();

            Set<String> excludedLogicalFields =
                    new LinkedHashSet<>();

            for (String blockName : excludedBlocks) {
                JsonNode blockNode =
                        blockNodes.get(blockName);

                if (blockNode == null || !blockNode.isObject()) {
                    continue;
                }

                Map<String, String> blockNormalized =
                        normalizeSingleBlock(
                                blockName,
                                blockNode
                        );

                excludedBlockNormalizedMaps.put(
                        blockName,
                        blockNormalized
                );

                excludedLogicalFields.addAll(
                        blockNormalized.keySet()
                );

                String blockDatasetCode =
                        text(
                                blockNode,
                                "dataset_code",
                                blockName
                        );

                excludedBlockDatasetCodes.put(
                        blockName,
                        blockDatasetCode
                );

                if (logPayloads && log.isInfoEnabled()) {
                    log.info(
                            "[PIPE][{}] 2) BLOCK_NORMALIZED_MAP block={} datasetCode={} full(masked):\n{}",
                            qid,
                            blockName,
                            blockDatasetCode,
                            prettyObject(maskMap(blockNormalized))
                    );
                }
            }

            Map<String, String> mainNormalizedMap =
                    removeKeys(
                            normalizedMap,
                            excludedLogicalFields
                    );

            Map<String, Set<String>> mainFieldToRules =
                    removeKeys(
                            allFieldToRules,
                            excludedLogicalFields
                    );

            Map<String, String> mainEffectiveNormalizedMap =
                    buildEffectiveNormalizedMap(
                            controlArea,
                            mainNormalizedMap,
                            mainFieldToRules
                    );

            Map<String, Set<String>> mainEffectiveFieldToRules =
                    buildEffectiveFieldToRules(
                            controlArea,
                            mainEffectiveNormalizedMap,
                            mainFieldToRules
                    );

            log.info(
                    "[DIAG][eventId={}] MAIN filterFlag={} normalized.size={} fieldToRules.fields={} fieldToRules.ruleRefs={}",
                    eventId,
                    cacheRuntime.filterFlag(controlArea),
                    mainEffectiveNormalizedMap.size(),
                    mainEffectiveFieldToRules.size(),
                    countRuleReferences(mainEffectiveFieldToRules)
            );

            logDiagnosticFields(
                    "MAIN_EFFECTIVE",
                    eventId,
                    mainEffectiveNormalizedMap,
                    mainEffectiveFieldToRules
            );

            ValidationResult mainValidation =
                    validationService.validate(
                            compiledRules,
                            mainEffectiveNormalizedMap,
                            mainEffectiveFieldToRules,
                            errorMessagesByRule
                    );

            log.info(
                    "[DIAG][eventId={}] MAIN validation allResult={} processStatus={} detailFields={} errorFields={}",
                    eventId,
                    mainValidation.allResult(),
                    mainValidation.processStatus(),
                    mainValidation.detailByField() == null
                            ? 0
                            : mainValidation.detailByField().size(),
                    mainValidation.errorsByField() == null
                            ? 0
                            : mainValidation.errorsByField().size()
            );

            logDiagnosticValidationResult(
                    "MAIN",
                    eventId,
                    mainValidation
            );

            Map<String, Map<String, String>> mergedDetailByField =
                    new LinkedHashMap<>();

            if (mainValidation.detailByField() != null) {
                mergedDetailByField.putAll(
                        mainValidation.detailByField()
                );
            }

            Map<String, List<String>> mergedErrorsByField =
                    new LinkedHashMap<>();

            mergeErrors(
                    mergedErrorsByField,
                    mainValidation.errorsByField()
            );

            Map<String, Map<String, Map<String, String>>> mergedDetailByDataset =
                    new LinkedHashMap<>();

            mergedDetailByDataset.put(
                    datasetCode,
                    safeFieldMap(mainValidation.detailByField())
            );

            boolean anyError =
                    RESULT_ERROR.equalsIgnoreCase(
                            mainValidation.allResult()
                    );

            boolean anyWarning =
                    RESULT_WARNING.equalsIgnoreCase(
                            mainValidation.allResult()
                    );

            boolean anyRuleException =
                    PROCESS_RULE_EXCEPTION.equalsIgnoreCase(
                            mainValidation.processStatus()
                    );

            for (String blockName : excludedBlocks) {
                JsonNode blockNode =
                        blockNodes.get(blockName);

                if (blockNode == null || !blockNode.isObject()) {
                    continue;
                }

                String blockDatasetCode =
                        excludedBlockDatasetCodes.getOrDefault(
                                blockName,
                                blockName
                        );

                String blockControlArea =
                        cacheRuntime.controlAreaByDataset(
                                blockDatasetCode
                        );

                if (blockControlArea == null
                        || blockControlArea.isBlank()) {
                    blockControlArea = controlArea;
                }

                Map<String, String> blockNormalizedMap =
                        excludedBlockNormalizedMaps
                                .getOrDefault(
                                        blockName,
                                        Map.of()
                                );

                Set<String> blockLogicalFields =
                        collectLogicalFieldsFromBlock(
                                blockNode
                        );

                blockLogicalFields.addAll(
                        blockNormalizedMap.keySet()
                );

                Map<String, Set<String>> blockFieldToRules =
                        selectKeys(
                                allFieldToRules,
                                blockLogicalFields
                        );

                Map<String, String> blockEffectiveNormalizedMap =
                        buildEffectiveNormalizedMap(
                                blockControlArea,
                                blockNormalizedMap,
                                blockFieldToRules
                        );

                Map<String, Set<String>> blockEffectiveFieldToRules =
                        buildEffectiveFieldToRules(
                                blockControlArea,
                                blockEffectiveNormalizedMap,
                                blockFieldToRules
                        );

                log.info(
                        "[DIAG][eventId={}] BLOCK={} datasetCode={} controlArea={} filterFlag={} normalized.size={} fieldToRules.fields={} fieldToRules.ruleRefs={}",
                        eventId,
                        blockName,
                        blockDatasetCode,
                        blockControlArea,
                        cacheRuntime.filterFlag(blockControlArea),
                        blockEffectiveNormalizedMap.size(),
                        blockEffectiveFieldToRules.size(),
                        countRuleReferences(blockEffectiveFieldToRules)
                );

                logDiagnosticFields(
                        "BLOCK=" + blockName,
                        eventId,
                        blockEffectiveNormalizedMap,
                        blockEffectiveFieldToRules
                );

                ValidationResult blockValidation =
                        validationService.validate(
                                compiledRules,
                                blockEffectiveNormalizedMap,
                                blockEffectiveFieldToRules,
                                errorMessagesByRule
                        );

                log.info(
                        "[DIAG][eventId={}] BLOCK={} validation allResult={} processStatus={} detailFields={} errorFields={}",
                        eventId,
                        blockName,
                        blockValidation.allResult(),
                        blockValidation.processStatus(),
                        blockValidation.detailByField() == null
                                ? 0
                                : blockValidation.detailByField().size(),
                        blockValidation.errorsByField() == null
                                ? 0
                                : blockValidation.errorsByField().size()
                );

                logDiagnosticValidationResult(
                        "BLOCK=" + blockName,
                        eventId,
                        blockValidation
                );

                if (blockValidation.detailByField() != null) {
                    mergedDetailByField.putAll(
                            blockValidation.detailByField()
                    );
                }

                mergeErrors(
                        mergedErrorsByField,
                        blockValidation.errorsByField()
                );

                mergedDetailByDataset.put(
                        blockDatasetCode,
                        safeFieldMap(
                                blockValidation.detailByField()
                        )
                );

                if (RESULT_ERROR.equalsIgnoreCase(
                        blockValidation.allResult())) {
                    anyError = true;
                } else if (RESULT_WARNING.equalsIgnoreCase(
                        blockValidation.allResult())) {
                    anyWarning = true;
                }

                if (PROCESS_RULE_EXCEPTION.equalsIgnoreCase(
                        blockValidation.processStatus())) {
                    anyRuleException = true;
                }
            }

            ValidationResult finalValidation =
                    new ValidationResult(
                            null,
                            resolveAllResult(
                                    anyError,
                                    anyWarning
                            ),
                            anyRuleException
                                    ? PROCESS_RULE_EXCEPTION
                                    : PROCESS_OK,
                            Map.copyOf(mergedDetailByField),
                            Map.copyOf(mergedDetailByDataset),
                            freezeErrors(mergedErrorsByField)
                    );

            log.info(
                    "[DIAG][eventId={}] FINAL allResult={} processStatus={} mergedDetailFields={} mergedDatasets={} mergedErrorFields={}",
                    eventId,
                    finalValidation.allResult(),
                    finalValidation.processStatus(),
                    finalValidation.detailByField() == null
                            ? 0
                            : finalValidation.detailByField().size(),
                    finalValidation.detailByDataset() == null
                            ? 0
                            : finalValidation.detailByDataset().size(),
                    finalValidation.errorsByField() == null
                            ? 0
                            : finalValidation.errorsByField().size()
            );

            logDiagnosticValidationResult(
                    "FINAL",
                    eventId,
                    finalValidation
            );

            String shortJson =
                    shortAnswerService.build(
                            originalEvent,
                            finalValidation,
                            qid,
                            in.createdDttm,
                            in.readedDttm
                    );

            if (shortJson == null) {
                log.warn(
                        "[PIPE][{}][eventId={}] ShortAnswerService returned null.",
                        qid,
                        eventId
                );
                return null;
            }

            if (logPayloads && log.isInfoEnabled()) {
                log.info(
                        "[PIPE][{}] 3) ANSWER_SHORT:\n{}",
                        qid,
                        maskJsonPretty(shortJson)
                );
            }

            String detailJson =
                    detailAnswerService.build(
                            originalEvent,
                            finalValidation,
                            qid,
                            in.createdDttm,
                            in.readedDttm,
                            excludedBlocks
                    );

            if (detailJson != null) {
                if (logPayloads && log.isInfoEnabled()) {
                    log.info(
                            "[PIPE][{}] 4) ANSWER_DETAIL:\n{}",
                            qid,
                            maskJsonPretty(detailJson)
                    );
                }
            } else {
                log.warn(
                        "[PIPE][{}][eventId={}] DetailAnswerService returned null.",
                        qid,
                        eventId
                );
            }

            return buildProcessingResult(
                    in,
                    shortJson,
                    detailJson,
                    raw
            );

        } catch (Exception e) {
            log.error(
                    "[PIPE][eventId={}] Failed to build answers.",
                    eventId,
                    e
            );
            return null;
        }
    }

    private static String resolveAllResult(
            boolean anyError,
            boolean anyWarning
    ) {
        if (anyError) {
            return RESULT_ERROR;
        }

        if (anyWarning) {
            return RESULT_WARNING;
        }

        return RESULT_SUCCESS;
    }

    private String extractFirstDatasetCode(
            JsonNode originalEvent
    ) {
        JsonNode dataNode =
                originalEvent.path("data");

        if (!dataNode.isObject()) {
            return null;
        }

        Iterator<Map.Entry<String, JsonNode>> fields =
                dataNode.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry =
                    fields.next();

            JsonNode child =
                    entry.getValue();

            if (child != null && child.isObject()) {
                JsonNode datasetCodeNode =
                        child.get("dataset_code");

                if (datasetCodeNode != null
                        && !datasetCodeNode.isNull()) {

                    String datasetCode =
                            datasetCodeNode.asText(null);

                    if (datasetCode != null
                            && !datasetCode.isBlank()) {
                        return datasetCode.trim();
                    }
                }
            }
        }

        return null;
    }

    private Map<String, JsonNode> collectTopLevelBlockNodes(
            JsonNode dataNode
    ) {
        Map<String, JsonNode> result =
                new LinkedHashMap<>();

        if (dataNode == null || !dataNode.isObject()) {
            return result;
        }

        Iterator<Map.Entry<String, JsonNode>> fields =
                dataNode.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry =
                    fields.next();

            if (entry.getValue() != null
                    && entry.getValue().isObject()) {
                result.put(
                        entry.getKey(),
                        entry.getValue()
                );
            }
        }

        return result;
    }

    private Map<String, String> normalizeSingleBlock(
            String blockName,
            JsonNode blockNode
    ) {
        ObjectNode root =
                mapper.createObjectNode();

        ObjectNode data =
                mapper.createObjectNode();

        data.set(
                blockName,
                blockNode
        );

        root.set(
                "data",
                data
        );

        return normalizer.normalize(root);
    }

    private Set<String> collectLogicalFieldsFromBlock(
            JsonNode blockNode
    ) {
        Set<String> out =
                new LinkedHashSet<>();

        if (blockNode == null
                || !blockNode.isObject()) {
            return out;
        }

        Iterator<Map.Entry<String, JsonNode>> it =
                blockNode.fields();

        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e =
                    it.next();

            String key =
                    e.getKey();

            JsonNode value =
                    e.getValue();

            if (key == null
                    || !key.startsWith("mapping.")
                    || value == null
                    || value.isNull()) {
                continue;
            }

            String logical =
                    isNullLikeText(value)
                            ? null
                            : value.asText(null);

            // В mapping.* значение "none" по-прежнему означает:
            // logical mapping отсутствует.
            if (logical == null
                    || logical.isBlank()
                    || "none".equalsIgnoreCase(
                    logical.trim()
            )) {
                continue;
            }

            out.add(logical.trim());
        }

        return out;
    }

    private <V> Map<String, V> removeKeys(
            Map<String, V> source,
            Set<String> keysToRemove
    ) {
        if (source == null
                || source.isEmpty()) {
            return new LinkedHashMap<>();
        }

        if (keysToRemove == null
                || keysToRemove.isEmpty()) {
            return new LinkedHashMap<>(source);
        }

        Map<String, V> result =
                new LinkedHashMap<>();

        for (Map.Entry<String, V> entry
                : source.entrySet()) {

            if (!keysToRemove.contains(
                    entry.getKey())) {
                result.put(
                        entry.getKey(),
                        entry.getValue()
                );
            }
        }

        return result;
    }

    private <V> Map<String, V> selectKeys(
            Map<String, V> source,
            Set<String> allowedKeys
    ) {
        Map<String, V> result =
                new LinkedHashMap<>();

        if (source == null
                || source.isEmpty()
                || allowedKeys == null
                || allowedKeys.isEmpty()) {
            return result;
        }

        for (Map.Entry<String, V> entry
                : source.entrySet()) {

            if (allowedKeys.contains(
                    entry.getKey())) {
                result.put(
                        entry.getKey(),
                        entry.getValue()
                );
            }
        }

        return result;
    }

    private Map<String, Set<String>> filterFieldToRulesByNormalizedMap(
            Map<String, Set<String>> fieldToRules,
            Map<String, String> normalizedMap
    ) {
        Map<String, Set<String>> result =
                new LinkedHashMap<>();

        if (fieldToRules == null
                || fieldToRules.isEmpty()
                || normalizedMap == null
                || normalizedMap.isEmpty()) {
            return result;
        }

        for (Map.Entry<String, Set<String>> entry
                : fieldToRules.entrySet()) {

            String logicalField =
                    entry.getKey();

            if (logicalField == null
                    || logicalField.isBlank()) {
                continue;
            }

            if (normalizedMap.containsKey(
                    logicalField)) {
                result.put(
                        logicalField,
                        entry.getValue()
                );
                continue;
            }

            String alt =
                    logicalField.replace('.', ',');

            if (normalizedMap.containsKey(alt)) {
                result.put(
                        logicalField,
                        entry.getValue()
                );
            }
        }

        return result;
    }

    private Map<String, String> buildEffectiveNormalizedMap(
            String controlArea,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRules
    ) {
        Map<String, String> safeNormalized =
                normalizedMap == null
                        ? new LinkedHashMap<>()
                        : new LinkedHashMap<>(
                        normalizedMap
                );

        Boolean filterFlag =
                cacheRuntime.filterFlag(
                        controlArea
                );

        if (!Boolean.TRUE.equals(
                filterFlag)) {
            return safeNormalized;
        }

        Map<String, String> effective =
                new LinkedHashMap<>();

        if (fieldToRules == null
                || fieldToRules.isEmpty()) {
            return safeNormalized;
        }

        for (String logicalField
                : fieldToRules.keySet()) {

            if (logicalField == null
                    || logicalField.isBlank()) {
                continue;
            }

            if (safeNormalized.containsKey(
                    logicalField)) {
                effective.put(
                        logicalField,
                        safeNormalized.get(
                                logicalField
                        )
                );
                continue;
            }

            String alt =
                    logicalField.replace('.', ',');

            if (safeNormalized.containsKey(alt)) {
                effective.put(
                        logicalField,
                        safeNormalized.get(alt)
                );
                continue;
            }

            effective.put(
                    logicalField,
                    null
            );
        }

        for (Map.Entry<String, String> entry
                : safeNormalized.entrySet()) {

            effective.putIfAbsent(
                    entry.getKey(),
                    entry.getValue()
            );
        }

        return effective;
    }

    private Map<String, Set<String>> buildEffectiveFieldToRules(
            String controlArea,
            Map<String, String> effectiveNormalizedMap,
            Map<String, Set<String>> fieldToRules
    ) {
        Boolean filterFlag =
                cacheRuntime.filterFlag(
                        controlArea
                );

        if (Boolean.TRUE.equals(
                filterFlag)) {

            return fieldToRules == null
                    ? new LinkedHashMap<>()
                    : new LinkedHashMap<>(
                    fieldToRules
            );
        }

        return filterFieldToRulesByNormalizedMap(
                fieldToRules,
                effectiveNormalizedMap
        );
    }

    private Map<String, Map<String, String>> safeFieldMap(
            Map<String, Map<String, String>> source
    ) {
        return source == null
                ? Map.of()
                : source;
    }

    private void mergeErrors(
            Map<String, List<String>> target,
            Map<String, List<String>> source
    ) {
        if (source == null
                || source.isEmpty()) {
            return;
        }

        for (Map.Entry<String, List<String>> entry
                : source.entrySet()) {

            String logicalField =
                    entry.getKey();

            List<String> messages =
                    entry.getValue();

            if (logicalField == null
                    || logicalField.isBlank()
                    || messages == null
                    || messages.isEmpty()) {
                continue;
            }

            LinkedHashSet<String> merged =
                    new LinkedHashSet<>(
                            target.getOrDefault(
                                    logicalField,
                                    List.of()
                            )
                    );

            for (String msg : messages) {
                if (msg != null
                        && !msg.isBlank()) {
                    merged.add(msg);
                }
            }

            if (!merged.isEmpty()) {
                target.put(
                        logicalField,
                        new ArrayList<>(merged)
                );
            }
        }
    }

    private Map<String, List<String>> freezeErrors(
            Map<String, List<String>> source
    ) {
        if (source == null
                || source.isEmpty()) {
            return Map.of();
        }

        Map<String, List<String>> result =
                new LinkedHashMap<>();

        for (Map.Entry<String, List<String>> entry
                : source.entrySet()) {

            if (entry.getKey() == null
                    || entry.getKey().isBlank()) {
                continue;
            }

            List<String> messages =
                    entry.getValue() == null
                            ? List.of()
                            : entry.getValue();

            result.put(
                    entry.getKey(),
                    List.copyOf(messages)
            );
        }

        return Map.copyOf(result);
    }

    private String prettyObject(Object o) {
        try {
            return mapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(o);
        } catch (Exception e) {
            return String.valueOf(o);
        }
    }

    private String maskInline(String s) {
        if (s == null) {
            return null;
        }

        return s
                .replaceAll(
                        "(\"birthdate\"\\s*:\\s*\")[^\"]*(\")",
                        "$1***$2"
                )
                .replaceAll(
                        "(\"clientSnils\"\\s*:\\s*\")[^\"]*(\")",
                        "$1***$2"
                )
                .replaceAll(
                        "(\"snils\"\\s*:\\s*\")[^\"]*(\")",
                        "$1***$2"
                )
                .replaceAll(
                        "(\"number\"\\s*:\\s*\")[^\"]*(\")",
                        "$1***$2"
                )
                .replaceAll(
                        "(\"series\"\\s*:\\s*\")[^\"]*(\")",
                        "$1***$2"
                )
                .replaceAll(
                        "(\"departmentCode\"\\s*:\\s*\")[^\"]*(\")",
                        "$1***$2"
                );
    }

    private String maskJsonPretty(String json) {
        if (json == null
                || json.isBlank()) {
            return json;
        }

        try {
            JsonNode root =
                    mapper.readTree(json);

            maskNode(root);

            return mapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(root);

        } catch (Exception e) {
            return maskInline(json);
        }
    }

    private void maskNode(JsonNode node) {
        if (node == null) {
            return;
        }

        if (node.isObject()) {
            Iterator<String> it =
                    node.fieldNames();

            while (it.hasNext()) {
                String fn =
                        it.next();

                JsonNode child =
                        node.get(fn);

                if (isSensitiveKey(fn)
                        && node instanceof ObjectNode obj) {

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

    private boolean isSensitiveKey(
            String key
    ) {
        if (key == null) {
            return false;
        }

        String k =
                key.toLowerCase(
                        Locale.ROOT
                );

        return k.equals("birthdate")
                || k.equals("clientsnils")
                || k.equals("snils")
                || k.equals("inn")
                || k.equals("number")
                || k.equals("series")
                || k.equals("departmentcode");
    }

    private Map<String, String> maskMap(
            Map<String, String> m
    ) {
        if (m == null) {
            return Map.of();
        }

        Map<String, String> out =
                new LinkedHashMap<>();

        for (Map.Entry<String, String> e
                : m.entrySet()) {

            String k =
                    e.getKey();

            String v =
                    e.getValue();

            if (k != null
                    && (isSensitiveKey(k)
                    || k.toLowerCase(Locale.ROOT)
                    .contains("snils")
                    || k.toLowerCase(Locale.ROOT)
                    .contains("birthdate")
                    || k.toLowerCase(Locale.ROOT)
                    .contains("passport")
                    || k.toLowerCase(Locale.ROOT)
                    .contains("number"))) {

                out.put(k, "***");

            } else {
                out.put(k, v);
            }
        }

        return out;
    }

    private String text(
            JsonNode node,
            String field,
            String def
    ) {
        if (node == null) {
            return def;
        }

        JsonNode v =
                node.get(field);

        return v == null || v.isNull()
                ? def
                : v.asText(def);
    }

    private ProcessingResult buildProcessingResult(
            MessageRecord in,
            String shortJson,
            String detailJson,
            String originalJson
    ) {
        if (in == null) {
            throw new IllegalArgumentException(
                    "MessageRecord is null"
            );
        }

        if (in.mqMessageId != null) {
            return ProcessingResult.forMq(
                    in.mqMessageId,
                    shortJson,
                    detailJson,
                    originalJson
            );
        }

        if (in.jmsMessageId != null
                && !in.jmsMessageId.isBlank()) {

            return ProcessingResult.forJms(
                    in.jmsMessageId,
                    shortJson,
                    detailJson,
                    originalJson
            );
        }

        throw new IllegalStateException(
                "MessageRecord has neither mqMessageId nor jmsMessageId. Cannot build ProcessingResult. "
                        + in
        );
    }

    private String extractEventId(
            MessageRecord in
    ) {
        if (in == null) {
            return "unknown";
        }

        if (in.mqMessageId != null) {
            return MessageRecord.mqIdToHex(
                    in.mqMessageId
            );
        }

        if (in.jmsMessageId != null
                && !in.jmsMessageId.isBlank()) {
            return in.jmsMessageId;
        }

        return "unknown";
    }

    /**
     * Нормализует только реально пустые строковые значения:
     *
     * ""     -> null
     * "   "  -> null
     *
     * ВАЖНО:
     * "none" НЕ преобразуется в null.
     *
     * Это сделано для совместимости со старым Python/.NET,
     * где is_null("none") == false.
     */
    private int normalizeEmptyStringsToNull(
            JsonNode node
    ) {
        if (node == null || node.isNull()) {
            return 0;
        }

        int converted = 0;

        if (node.isObject()) {
            ObjectNode objectNode =
                    (ObjectNode) node;

            Iterator<Map.Entry<String, JsonNode>> fields =
                    objectNode.fields();

            List<String> fieldsToNull =
                    new ArrayList<>();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry =
                        fields.next();

                JsonNode child =
                        entry.getValue();

                if (isNullLikeText(child)) {
                    fieldsToNull.add(
                            entry.getKey()
                    );
                } else {
                    converted +=
                            normalizeEmptyStringsToNull(
                                    child
                            );
                }
            }

            for (String fieldName
                    : fieldsToNull) {

                objectNode.set(
                        fieldName,
                        mapper.nullNode()
                );

                converted++;
            }

            return converted;
        }

        if (node.isArray()) {
            for (JsonNode child : node) {
                converted +=
                        normalizeEmptyStringsToNull(
                                child
                        );
            }
        }

        return converted;
    }

    private void logNullLikeInputDiagnostics(
            JsonNode originalEvent,
            String eventId
    ) {
        if (originalEvent == null) {
            return;
        }

        JsonNode data =
                originalEvent.path("data");

        logInputValueState(
                eventId,
                "documents.clientInn",
                data.path("documents")
                        .get("clientInn")
        );

        logInputValueState(
                eventId,
                "documents.clientSnils",
                data.path("documents")
                        .get("clientSnils")
        );

        logInputValueState(
                eventId,
                "contactInfo.emailValue",
                data.path("contactInfo")
                        .get("emailValue")
        );

        logInputValueState(
                eventId,
                "homeAddress.area",
                data.path("homeAddress")
                        .get("area")
        );

        logInputValueState(
                eventId,
                "homeAddress.block",
                data.path("homeAddress")
                        .get("block")
        );

        logInputValueState(
                eventId,
                "registrationAddress.area",
                data.path("registrationAddress")
                        .get("area")
        );

        logInputValueState(
                eventId,
                "registrationAddress.block",
                data.path("registrationAddress")
                        .get("block")
        );
    }

    private void logInputValueState(
            String eventId,
            String path,
            JsonNode value
    ) {
        log.info(
                "[DIAG][eventId={}] INPUT {} state={}",
                eventId,
                path,
                valueState(value)
        );
    }

    private String valueState(
            JsonNode value
    ) {
        if (value == null
                || value.isMissingNode()) {
            return "MISSING";
        }

        if (value.isNull()) {
            return "NULL";
        }

        if (!value.isTextual()) {
            return "PRESENT_"
                    + value.getNodeType();
        }

        String text =
                value.asText();

        if (text == null) {
            return "NULL_TEXT";
        }

        if (text.isBlank()) {
            return "BLANK";
        }

        if ("none".equalsIgnoreCase(
                text.trim())) {
            return "NONE";
        }

        return "PRESENT";
    }

    private void logDiagnosticFields(
            String stage,
            String eventId,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRules
    ) {
        for (String logicalField
                : DIAG_LOGICAL_FIELDS) {

            boolean normalizedContains =
                    normalizedMap != null
                            && normalizedMap
                            .containsKey(logicalField);

            String valueState =
                    normalizedContains
                            ? stringValueState(
                            normalizedMap.get(
                                    logicalField
                            )
                    )
                            : "MISSING";

            Set<String> rules =
                    fieldToRules == null
                            ? null
                            : fieldToRules.get(
                            logicalField
                    );

            log.info(
                    "[DIAG][eventId={}] {} field='{}' normalizedContains={} valueState={} rules={}",
                    eventId,
                    stage,
                    logicalField,
                    normalizedContains,
                    valueState,
                    rules
            );
        }
    }

    private String stringValueState(
            String value
    ) {
        if (value == null) {
            return "NULL";
        }

        if (value.isBlank()) {
            return "BLANK";
        }

        if ("none".equalsIgnoreCase(
                value.trim())) {
            return "NONE";
        }

        return "PRESENT";
    }

    private int countRuleReferences(
            Map<String, Set<String>> fieldToRules
    ) {
        if (fieldToRules == null
                || fieldToRules.isEmpty()) {
            return 0;
        }

        int count = 0;

        for (Set<String> rules
                : fieldToRules.values()) {

            if (rules != null) {
                count += rules.size();
            }
        }

        return count;
    }

    private Map<String, Boolean> diagnosticRulePresence(
            Map<String, Rule> compiledRules
    ) {
        Map<String, Boolean> result =
                new LinkedHashMap<>();

        for (String ruleId
                : DIAG_RULE_IDS) {

            boolean present = false;

            if (compiledRules != null) {
                present =
                        compiledRules.containsKey(
                                ruleId
                        )
                                || compiledRules.containsKey(
                                "Rule" + ruleId
                        );
            }

            result.put(
                    ruleId,
                    present
            );
        }

        return result;
    }

    private void logMissingCompiledRules(
            String eventId,
            Map<String, Set<String>> fieldToRules,
            Map<String, Rule> compiledRules
    ) {
        if (fieldToRules == null
                || fieldToRules.isEmpty()) {
            return;
        }

        Set<String> missing =
                new LinkedHashSet<>();

        int totalReferenced = 0;

        for (Set<String> ruleIds
                : fieldToRules.values()) {

            if (ruleIds == null) {
                continue;
            }

            for (String ruleId : ruleIds) {
                if (ruleId == null
                        || ruleId.isBlank()) {
                    continue;
                }

                totalReferenced++;

                boolean present =
                        compiledRules != null
                                && (
                                compiledRules.containsKey(
                                        ruleId
                                )
                                        || compiledRules.containsKey(
                                        ruleId.startsWith("Rule")
                                                ? ruleId.substring(4)
                                                : "Rule" + ruleId
                                )
                        );

                if (!present) {
                    missing.add(ruleId);
                }
            }
        }

        log.info(
                "[DIAG][eventId={}] compiled rule coverage referenced={} missing.count={} missing.sample={}",
                eventId,
                totalReferenced,
                missing.size(),
                missing.stream()
                        .limit(30)
                        .toList()
        );
    }

    private void logDiagnosticValidationResult(
            String stage,
            String eventId,
            ValidationResult validation
    ) {
        if (validation == null
                || validation.detailByField() == null) {
            return;
        }

        for (String logicalField
                : DIAG_LOGICAL_FIELDS) {

            Map<String, String> ruleStatuses =
                    validation.detailByField()
                            .get(logicalField);

            if (ruleStatuses == null
                    || ruleStatuses.isEmpty()) {

                log.info(
                        "[DIAG][eventId={}] {} RESULT field='{}' absent",
                        eventId,
                        stage,
                        logicalField
                );

                continue;
            }

            Map<String, String> selected =
                    new LinkedHashMap<>();

            for (Map.Entry<String, String> entry
                    : ruleStatuses.entrySet()) {

                String ruleId =
                        normalizeRuleId(
                                entry.getKey()
                        );

                if (DIAG_RULE_IDS.contains(
                        ruleId)) {

                    selected.put(
                            entry.getKey(),
                            entry.getValue()
                    );
                }
            }

            if (!selected.isEmpty()) {
                log.info(
                        "[DIAG][eventId={}] {} RESULT field='{}' statuses={}",
                        eventId,
                        stage,
                        logicalField,
                        selected
                );
            }
        }
    }

    private String normalizeRuleId(
            String ruleName
    ) {
        if (ruleName == null) {
            return "";
        }

        return ruleName.startsWith("Rule")
                && ruleName.length() > 4
                ? ruleName.substring(4)
                : ruleName;
    }

    /**
     * ВАЖНО:
     *
     * Старый Python/.NET считает null только:
     * - None
     * - ""
     * - специальный CNULLVal
     *
     * Строка "none" НЕ является null.
     *
     * Поэтому здесь "none" намеренно не проверяем.
     */

    private boolean isNullLikeText(JsonNode node) {
        if (node == null || !node.isTextual()) {
            return false;
        }

        String text = node.asText();
        return text == null || text.isEmpty();
    }
}