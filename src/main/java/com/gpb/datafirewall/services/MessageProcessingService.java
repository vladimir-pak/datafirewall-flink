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

    private static final Logger log = LoggerFactory.getLogger(MessageProcessingService.class);

    private static final String RESULT_ERROR = "ERROR";
    private static final String RESULT_WARNING = "WARNING";
    private static final String RESULT_SUCCESS = "SUCCESS";
    private static final String PROCESS_RULE_EXCEPTION = "RULE_EXCEPTION";
    private static final String PROCESS_OK = "OK";

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
            normalizeEmptyStringsToNull(originalEvent);

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
                log.info("[PIPE][{}][eventId={}] 1) INBOUND:\n{}", qid, eventId, maskJsonPretty(raw));
            }

            String datasetCode = extractFirstDatasetCode(originalEvent);
            if (datasetCode == null || datasetCode.isBlank()) {
                log.warn("[PIPE][{}][eventId={}] dataset_code not found in input payload", qid, eventId);
                return null;
            }

            String controlArea = cacheRuntime.controlAreaByDataset(datasetCode);
            if (controlArea == null || controlArea.isBlank()) {
                log.warn("[PIPE][{}][eventId={}] controlArea not found for datasetCode={}", qid, eventId, datasetCode);
                return null;
            }

            Map<String, Set<String>> allFieldToRules = cacheRuntime.fieldToRules(controlArea);
            if (allFieldToRules == null || allFieldToRules.isEmpty()) {
                log.warn("[PIPE][{}][eventId={}] fieldToRules not found for controlArea={} datasetCode={}",
                        qid, eventId, controlArea, datasetCode);
                return null;
            }

            Map<String, String> normalizedMap = normalizer.normalize(originalEvent);
            Map<String, Map<String, String>> errorMessagesByRule = cacheRuntime.errorMessagesSnapshot();

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 2) NORMALIZED_MAP size={} keys={}",
                        qid, normalizedMap.size(), normalizedMap.keySet());
                log.info("[PIPE][{}] 2) NORMALIZED_MAP full(masked):\n{}",
                        qid,
                        prettyObject(maskMap(normalizedMap)));
            }

            Map<String, Rule> compiledRules = cacheRuntime.rulesSnapshot();

            Set<String> excludedBlocks = cacheRuntime.excludedBlocks(controlArea);

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

            Map<String, String> mainNormalizedMap =
                    removeKeys(normalizedMap, excludedLogicalFields);

            Map<String, Set<String>> mainFieldToRules =
                    removeKeys(allFieldToRules, excludedLogicalFields);

            Map<String, String> mainEffectiveNormalizedMap =
                    buildEffectiveNormalizedMap(controlArea, mainNormalizedMap, mainFieldToRules);

            Map<String, Set<String>> mainEffectiveFieldToRules =
                    buildEffectiveFieldToRules(controlArea, mainEffectiveNormalizedMap, mainFieldToRules);

            ValidationResult mainValidation = validationService.validate(
                    compiledRules,
                    mainEffectiveNormalizedMap,
                    mainEffectiveFieldToRules,
                    errorMessagesByRule
            );

            Map<String, Map<String, String>> mergedDetailByField = new LinkedHashMap<>();
            if (mainValidation.detailByField() != null) {
                mergedDetailByField.putAll(mainValidation.detailByField());
            }

            Map<String, List<String>> mergedErrorsByField = new LinkedHashMap<>();
            mergeErrors(mergedErrorsByField, mainValidation.errorsByField());

            Map<String, Map<String, Map<String, String>>> mergedDetailByDataset = new LinkedHashMap<>();
            mergedDetailByDataset.put(datasetCode, safeFieldMap(mainValidation.detailByField()));

            boolean anyError = RESULT_ERROR.equalsIgnoreCase(mainValidation.allResult());
            boolean anyWarning = RESULT_WARNING.equalsIgnoreCase(mainValidation.allResult());
            boolean anyRuleException = PROCESS_RULE_EXCEPTION.equalsIgnoreCase(mainValidation.processStatus());

            for (String blockName : excludedBlocks) {
                JsonNode blockNode = blockNodes.get(blockName);
                if (blockNode == null || !blockNode.isObject()) {
                    continue;
                }

                String blockDatasetCode = excludedBlockDatasetCodes.getOrDefault(blockName, blockName);
                String blockControlArea = cacheRuntime.controlAreaByDataset(blockDatasetCode);
                if (blockControlArea == null || blockControlArea.isBlank()) {
                    blockControlArea = controlArea;
                }

                Map<String, String> blockNormalizedMap =
                        excludedBlockNormalizedMaps.getOrDefault(blockName, Map.of());

                Set<String> blockLogicalFields = collectLogicalFieldsFromBlock(blockNode);
                blockLogicalFields.addAll(blockNormalizedMap.keySet());

                Map<String, Set<String>> blockFieldToRules =
                        selectKeys(allFieldToRules, blockLogicalFields);

                Map<String, String> blockEffectiveNormalizedMap =
                        buildEffectiveNormalizedMap(blockControlArea, blockNormalizedMap, blockFieldToRules);

                Map<String, Set<String>> blockEffectiveFieldToRules =
                        buildEffectiveFieldToRules(blockControlArea, blockEffectiveNormalizedMap, blockFieldToRules);

                ValidationResult blockValidation = validationService.validate(
                        compiledRules,
                        blockEffectiveNormalizedMap,
                        blockEffectiveFieldToRules,
                        errorMessagesByRule
                );

                if (blockValidation.detailByField() != null) {
                    mergedDetailByField.putAll(blockValidation.detailByField());
                }
                mergeErrors(mergedErrorsByField, blockValidation.errorsByField());

                mergedDetailByDataset.put(blockDatasetCode, safeFieldMap(blockValidation.detailByField()));

                if (RESULT_ERROR.equalsIgnoreCase(blockValidation.allResult())) {
                    anyError = true;
                } else if (RESULT_WARNING.equalsIgnoreCase(blockValidation.allResult())) {
                    anyWarning = true;
                }

                if (PROCESS_RULE_EXCEPTION.equalsIgnoreCase(blockValidation.processStatus())) {
                    anyRuleException = true;
                }
            }

            ValidationResult finalValidation = new ValidationResult(
                    null,
                    resolveAllResult(anyError, anyWarning),
                    anyRuleException ? PROCESS_RULE_EXCEPTION : PROCESS_OK,
                    Map.copyOf(mergedDetailByField),
                    Map.copyOf(mergedDetailByDataset),
                    freezeErrors(mergedErrorsByField)
            );

            String shortJson = shortAnswerService.build(originalEvent, finalValidation, qid, in.createdDttm, in.readedDttm);
            if (shortJson == null) {
                log.warn("[PIPE][{}][eventId={}] ShortAnswerService returned null.", qid, eventId);
                return null;
            }

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 3) ANSWER_SHORT:\n{}", qid, maskJsonPretty(shortJson));
            }

            String detailJson = detailAnswerService.build(originalEvent, finalValidation, qid, in.createdDttm, in.readedDttm, excludedBlocks);
            if (detailJson != null) {
                if (logPayloads && log.isInfoEnabled()) {
                    log.info("[PIPE][{}] 4) ANSWER_DETAIL:\n{}", qid, maskJsonPretty(detailJson));
                }
            } else {
                log.warn("[PIPE][{}][eventId={}] DetailAnswerService returned null.", qid, eventId);
            }

            ProcessingResult result = buildProcessingResult(
                    in,
                    shortJson,
                    detailJson,
                    raw
            );

            // log.info(
            //         "[PIPE][{}][eventId={}] result built: isMq={}, isJms={}, shortLen={}, detailLen={}",
            //         qid,
            //         result.getEventId(),
            //         result.isMq(),
            //         result.isJms(),
            //         result.getShortJson() == null ? 0 : result.getShortJson().length(),
            //         result.getDetailJson() == null ? 0 : result.getDetailJson().length()
            // );

            return result;

        } catch (Exception e) {
            log.error("[PIPE][eventId={}] Failed to build answers.", eventId, e);
            return null;
        }
    }

    private static String resolveAllResult(boolean anyError, boolean anyWarning) {
        if (anyError) {
            return RESULT_ERROR;
        }
        if (anyWarning) {
            return RESULT_WARNING;
        }
        return RESULT_SUCCESS;
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
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = mapper.createObjectNode();
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

            String logical = isNullLikeText(value) ? null : value.asText(null);
            if (logical == null || logical.isBlank() || "none".equalsIgnoreCase(logical.trim())) {
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
        if (source == null || source.isEmpty()) {
            return new LinkedHashMap<>();
        }

        if (keysToRemove == null || keysToRemove.isEmpty()) {
            return new LinkedHashMap<>(source);
        }

        Map<String, V> result = new LinkedHashMap<>();

        for (Map.Entry<String, V> entry : source.entrySet()) {
            if (!keysToRemove.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }

    private <V> Map<String, V> selectKeys(
            Map<String, V> source,
            Set<String> allowedKeys
    ) {
        Map<String, V> result = new LinkedHashMap<>();
        if (source == null || source.isEmpty() || allowedKeys == null || allowedKeys.isEmpty()) {
            return result;
        }

        for (Map.Entry<String, V> entry : source.entrySet()) {
            if (allowedKeys.contains(entry.getKey())) {
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

        Boolean filterFlag = cacheRuntime.filterFlag(controlArea);

        if (!Boolean.TRUE.equals(filterFlag)) {
            return safeNormalized;
        }

        Map<String, String> effective = new LinkedHashMap<>();

        if (fieldToRules == null || fieldToRules.isEmpty()) {
            return safeNormalized;
        }

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
        Boolean filterFlag = cacheRuntime.filterFlag(controlArea);

        if (Boolean.TRUE.equals(filterFlag)) {
            return fieldToRules == null ? new LinkedHashMap<>() : new LinkedHashMap<>(fieldToRules);
        }

        return filterFieldToRulesByNormalizedMap(fieldToRules, effectiveNormalizedMap);
    }

    private Map<String, Map<String, String>> safeFieldMap(Map<String, Map<String, String>> source) {
        return source == null ? Map.of() : source;
    }

    private void mergeErrors(
            Map<String, List<String>> target,
            Map<String, List<String>> source
    ) {
        if (source == null || source.isEmpty()) {
            return;
        }

        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            String logicalField = entry.getKey();
            List<String> messages = entry.getValue();

            if (logicalField == null || logicalField.isBlank() || messages == null || messages.isEmpty()) {
                continue;
            }

            LinkedHashSet<String> merged = new LinkedHashSet<>(target.getOrDefault(logicalField, List.of()));
            for (String msg : messages) {
                if (msg != null && !msg.isBlank()) {
                    merged.add(msg);
                }
            }

            if (!merged.isEmpty()) {
                target.put(logicalField, new ArrayList<>(merged));
            }
        }
    }

    private Map<String, List<String>> freezeErrors(Map<String, List<String>> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }

        Map<String, List<String>> result = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                continue;
            }
            List<String> messages = entry.getValue() == null ? List.of() : entry.getValue();
            result.put(entry.getKey(), List.copyOf(messages));
        }
        return Map.copyOf(result);
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
                if (isSensitiveKey(fn) && node instanceof ObjectNode obj) {
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

    private ProcessingResult buildProcessingResult(
            MessageRecord in,
            String shortJson,
            String detailJson,
            String originalJson
    ) {
        if (in == null) {
            throw new IllegalArgumentException("MessageRecord is null");
        }

        if (in.mqMessageId != null) {
            return ProcessingResult.forMq(
                    in.mqMessageId,
                    shortJson,
                    detailJson,
                    originalJson
            );
        }

        if (in.jmsMessageId != null && !in.jmsMessageId.isBlank()) {
            return ProcessingResult.forJms(
                    in.jmsMessageId,
                    shortJson,
                    detailJson,
                    originalJson
            );
        }

        throw new IllegalStateException(
                "MessageRecord has neither mqMessageId nor jmsMessageId. Cannot build ProcessingResult. " + in
        );
    }

    private String extractEventId(MessageRecord in) {
        if (in == null) {
            return "unknown";
        }

        if (in.mqMessageId != null) {
            return MessageRecord.mqIdToHex(in.mqMessageId);
        }

        if (in.jmsMessageId != null && !in.jmsMessageId.isBlank()) {
            return in.jmsMessageId;
        }

        return "unknown";
    }

    private void normalizeEmptyStringsToNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return;
        }

        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;

            Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            List<String> fieldsToNull = new ArrayList<>();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                JsonNode child = entry.getValue();

                if (isNullLikeText(child)) {
                    fieldsToNull.add(entry.getKey());
                } else {
                    normalizeEmptyStringsToNull(child);
                }
            }

            for (String fieldName : fieldsToNull) {
                objectNode.set(fieldName, mapper.nullNode());
            }

            return;
        }

        if (node.isArray()) {
            for (JsonNode child : node) {
                normalizeEmptyStringsToNull(child);
            }
        }
    }

    private boolean isNullLikeText(JsonNode node) {
        if (node == null || !node.isTextual()) {
            return false;
        }
        String text = node.asText();
        return text == null || text.isBlank() || "none".equalsIgnoreCase(text.trim());
    }
}
