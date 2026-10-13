package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.model.Rule;
import com.gpb.datafirewall.validation.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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

    public MessageProcessingService(ObjectMapper mapper, RulesCacheRuntime cacheRuntime, boolean logPayloads) {
        this.mapper = mapper;
        this.cacheRuntime = cacheRuntime;
        this.logPayloads = logPayloads;
        this.validationService = new ValidationService();
        this.shortAnswerService = new ShortAnswerService(mapper);
        this.detailAnswerService = new DetailAnswerService(mapper);
        this.normalizer = new MappingNormalizer(mapper);
    }

    public ProcessingResult process(MessageRecord in) {
        // Одна volatile-read на сообщение. Все дальнейшие lookup выполняются
        // по одному immutable snapshot без блокировки потока обработки.
        return processMessage(in, cacheRuntime.snapshot());
    }

    private ProcessingResult processMessage(
            MessageRecord in,
            RulesCacheSnapshot cacheSnapshot
    ) {
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
            if (qid == null || qid.isBlank()) qid = eventId;

            log.debug("[PIPE][{}][eventId={}] handler=flink", qid, eventId);

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}][eventId={}] 1) INBOUND:\n{}", qid, eventId, maskJsonPretty(raw));
            }

            String datasetCode = extractFirstDatasetCode(originalEvent);

            if (datasetCode == null || datasetCode.isBlank()) {
                log.warn("[PIPE][{}][eventId={}] dataset_code not found in input payload", qid, eventId);
                return null;
            }

            log.debug("[PIPE][{}][eventId={}] resolved datasetCode={}", qid, eventId, datasetCode);

            String controlArea = cacheSnapshot.controlAreaByDataset(datasetCode);

            if (controlArea == null || controlArea.isBlank()) {
                log.warn("[PIPE][{}][eventId={}] controlArea not found for datasetCode={}", qid, eventId, datasetCode);
                return null;
            }

            Map<String, Set<String>> allFieldToRules = cacheSnapshot.fieldToRules(controlArea);

            if (allFieldToRules == null || allFieldToRules.isEmpty()) {
                log.warn("[PIPE][{}][eventId={}] fieldToRules not found for controlArea={} datasetCode={}", qid, eventId, controlArea, datasetCode);
                return null;
            }

            Map<String, Map<String, String>> errorMessagesByRule = cacheSnapshot.errorMessagesByRule();
            // Reuse these immutable snapshots for all blocks of this message.
            Map<String, String> genderMapping = cacheSnapshot.genderMapping();
            Map<String, String> typeDulMapping = cacheSnapshot.typeDulMapping();
            Boolean filterFlag = cacheSnapshot.filterFlag(controlArea);
            Map<String, String> normalizedMap = normalizer.normalize(
                    originalEvent,
                    genderMapping,
                    typeDulMapping
            );

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 2) RAW_NORMALIZED_MAP size={} keys={}", qid, normalizedMap.size(), normalizedMap.keySet());
                log.info("[PIPE][{}] 2) RAW_NORMALIZED_MAP full(masked):\n{}", qid, prettyObject(maskMap(normalizedMap)));
            }

            Map<String, Rule> compiledRules = cacheSnapshot.compiledRules();
            Set<String> excludedBlocks = cacheSnapshot.excludedBlocks(controlArea);
            JsonNode dataNode = originalEvent.path("data");

            TopLevelPreparation topLevelPreparation = prepareTopLevelBlocks(
                    dataNode,
                    datasetCode,
                    controlArea,
                    allFieldToRules,
                    cacheSnapshot,
                    genderMapping,
                    typeDulMapping
            );
            Map<String, JsonNode> blockNodes = topLevelPreparation.objectBlocks();
            Set<String> arrayLogicalFields = topLevelPreparation.arrayLogicalFields();
            Set<String> completionControlAreas =
                    new LinkedHashSet<>(topLevelPreparation.completionControlAreas());
            if (Boolean.TRUE.equals(filterFlag)) {
                completionControlAreas.add(controlArea);
            }

            List<PreparedExcludedBlock> preparedExcludedBlocks =
                    new ArrayList<>(excludedBlocks.size());
            Set<String> excludedLogicalFields = new LinkedHashSet<>();

            for (String blockName : excludedBlocks) {
                JsonNode blockNode = blockNodes.get(blockName);
                if (blockNode == null || !blockNode.isObject()) continue;

                String blockDatasetCode = text(blockNode, "dataset_code", blockName);
                String blockControlArea = cacheSnapshot.controlAreaByDataset(blockDatasetCode);
                if (blockControlArea == null || blockControlArea.isBlank()) blockControlArea = controlArea;

                Map<String, Set<String>> blockAllFieldToRules = cacheSnapshot.fieldToRules(blockControlArea);
                String blockRulesControlArea = blockControlArea;
                if (blockAllFieldToRules == null || blockAllFieldToRules.isEmpty()) {
                    blockAllFieldToRules = allFieldToRules;
                    blockRulesControlArea = controlArea;
                }

                Boolean blockFilterFlag = cacheSnapshot.filterFlag(blockControlArea);
                Set<String> blockReferencedFields =
                        cacheSnapshot.referencedFields(blockRulesControlArea);
                if (Boolean.TRUE.equals(blockFilterFlag)) {
                    completionControlAreas.add(blockRulesControlArea);
                }

                Map<String, String> blockNormalized = normalizeSingleBlock(
                        blockName,
                        blockNode,
                        genderMapping,
                        typeDulMapping
                );

                Set<String> blockLogicalFields = collectLogicalFieldsFromBlock(blockNode);
                blockLogicalFields.addAll(blockNormalized.keySet());
                if (Boolean.TRUE.equals(blockFilterFlag)) {
                    // Эти ключи нужны для определения принадлежности поля
                    // локальному блоку, но не являются источником запуска.
                    blockLogicalFields.addAll(blockAllFieldToRules.keySet());
                }

                Map<String, Set<String>> blockExpectedFieldToRules = Boolean.TRUE.equals(blockFilterFlag)
                        ? blockAllFieldToRules
                        : selectKeys(blockAllFieldToRules, blockLogicalFields);

                EffectiveNormalization blockEffective = buildEffectiveNormalization(
                        blockFilterFlag,
                        blockNormalized,
                        blockExpectedFieldToRules,
                        blockReferencedFields,
                        blockLogicalFields
                );

                preparedExcludedBlocks.add(new PreparedExcludedBlock(
                        blockDatasetCode,
                        blockEffective.normalizedMap(),
                        blockEffective.fieldToRules()
                ));
                excludedLogicalFields.addAll(blockLogicalFields);
                if (!Boolean.TRUE.equals(blockFilterFlag)) {
                    excludedLogicalFields.addAll(blockEffective.normalizedMap().keySet());
                }

                if (logPayloads && log.isInfoEnabled()) {
                    log.info("[PIPE][{}] 2) BLOCK_NORMALIZED_MAP block={} datasetCode={} full(masked):\n{}", qid, blockName, blockDatasetCode, prettyObject(maskMap(blockEffective.normalizedMap())));
                }
            }

            Set<String> separatedLogicalFields = new LinkedHashSet<>(excludedLogicalFields);
            separatedLogicalFields.addAll(arrayLogicalFields);

            Map<String, String> mainNormalizedMap = removeKeys(normalizedMap, separatedLogicalFields);
            Map<String, Set<String>> mainFieldToRules = removeKeys(allFieldToRules, separatedLogicalFields);
            EffectiveNormalization mainEffective = buildEffectiveNormalization(
                    filterFlag,
                    mainNormalizedMap,
                    mainFieldToRules,
                    cacheSnapshot.referencedFields(controlArea),
                    Set.of()
            );

            /*
             * fieldToRules определяет только набор запускаемых правил, а
             * Rule.apply всегда получает полный контекст сообщения. Карта
             * строится один раз и дополняется отсутствующими зависимостями из
             * всех обнаруженных control area. Для отдельных блоков и
             * элементов массива ниже используется дешёвый overlay-view.
             */
            Map<String, String> completeRuleInput = buildCompleteRuleInput(
                    normalizedMap,
                    mainEffective,
                    preparedExcludedBlocks,
                    topLevelPreparation.arrayItems(),
                    completionControlAreas,
                    cacheSnapshot
            );

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 2) MAIN_NORMALIZED_MAP size={} keys={}", qid, mainEffective.normalizedMap().size(), mainEffective.normalizedMap().keySet());
                log.info("[PIPE][{}] 2) MAIN_NORMALIZED_MAP full(masked):\n{}", qid, prettyObject(maskMap(mainEffective.normalizedMap())));
            }

            ValidationResult mainValidation = validationService.validate(
                    compiledRules, completeRuleInput, mainEffective.fieldToRules(),
                    errorMessagesByRule);

            Map<String, Map<String, String>> mergedDetailByField = new LinkedHashMap<>();
            if (mainValidation.detailByField() != null) mergedDetailByField.putAll(mainValidation.detailByField());

            Map<String, List<String>> mergedErrorsByField = new LinkedHashMap<>();
            mergeErrors(mergedErrorsByField, mainValidation.errorsByField());

            Map<String, Map<String, Map<String, String>>> mergedDetailByDataset = new LinkedHashMap<>();
            mergedDetailByDataset.put(datasetCode, safeFieldMap(mainValidation.detailByField()));

            boolean anyError = RESULT_ERROR.equalsIgnoreCase(mainValidation.allResult());
            boolean anyWarning = RESULT_WARNING.equalsIgnoreCase(mainValidation.allResult());
            boolean anyRuleException = PROCESS_RULE_EXCEPTION.equalsIgnoreCase(mainValidation.processStatus());

            for (PreparedExcludedBlock block : preparedExcludedBlocks) {
                Map<String, String> blockRuleInput = scopedRuleInput(
                        completeRuleInput,
                        block.normalizedMap()
                );
                ValidationResult blockValidation = validationService.validate(
                        compiledRules, blockRuleInput, block.fieldToRules(),
                        errorMessagesByRule);

                if (blockValidation.detailByField() != null) mergedDetailByField.putAll(blockValidation.detailByField());

                mergeErrors(mergedErrorsByField, blockValidation.errorsByField());
                mergedDetailByDataset.put(block.datasetCode(), safeFieldMap(blockValidation.detailByField()));

                if (RESULT_ERROR.equalsIgnoreCase(blockValidation.allResult())) anyError = true;
                else if (RESULT_WARNING.equalsIgnoreCase(blockValidation.allResult())) anyWarning = true;

                if (PROCESS_RULE_EXCEPTION.equalsIgnoreCase(blockValidation.processStatus())) anyRuleException = true;
            }

            for (PreparedArrayItem item : topLevelPreparation.arrayItems()) {
                Map<String, String> itemRuleInput = scopedRuleInput(
                        completeRuleInput,
                        item.normalizedMap()
                );
                ValidationResult itemValidation = validationService.validate(
                        compiledRules, itemRuleInput, item.fieldToRules(),
                        errorMessagesByRule);

                String instanceKey = item.instanceKey();
                mergedDetailByDataset.put(instanceKey, safeFieldMap(itemValidation.detailByField()));
                mergeErrorsWithPrefix(mergedErrorsByField, instanceKey, itemValidation.errorsByField());

                if (RESULT_ERROR.equalsIgnoreCase(itemValidation.allResult())) anyError = true;
                else if (RESULT_WARNING.equalsIgnoreCase(itemValidation.allResult())) anyWarning = true;

                if (PROCESS_RULE_EXCEPTION.equalsIgnoreCase(itemValidation.processStatus())) anyRuleException = true;
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

            return buildProcessingResult(in, shortJson, detailJson, raw);

        } catch (Exception e) {
            log.error("[PIPE][eventId={}] Failed to build answers.", eventId, e);
            return null;
        }
    }

    private static String resolveAllResult(boolean anyError, boolean anyWarning) {
        if (anyError) return RESULT_ERROR;
        if (anyWarning) return RESULT_WARNING;
        return RESULT_SUCCESS;
    }

    private String extractFirstDatasetCode(JsonNode originalEvent) {
        if (originalEvent == null || originalEvent.isNull()) return null;

        JsonNode dataNode = originalEvent.path("data");
        if (!dataNode.isObject()) return null;

        return findFirstDatasetCode(dataNode);
    }

    private String findFirstDatasetCode(JsonNode node) {
        if (node == null || node.isNull()) return null;

        if (node.isObject()) {
            JsonNode datasetCodeNode = node.get("dataset_code");

            if (datasetCodeNode != null && !datasetCodeNode.isNull()) {
                String datasetCode = datasetCodeNode.asText(null);
                if (datasetCode != null && !datasetCode.isBlank()) return datasetCode.trim();
            }

            Iterator<JsonNode> elements = node.elements();

            while (elements.hasNext()) {
                String datasetCode = findFirstDatasetCode(elements.next());
                if (datasetCode != null && !datasetCode.isBlank()) return datasetCode;
            }

            return null;
        }

        if (node.isArray()) {
            for (JsonNode child : node) {
                String datasetCode = findFirstDatasetCode(child);
                if (datasetCode != null && !datasetCode.isBlank()) return datasetCode;
            }
        }

        return null;
    }

    private TopLevelPreparation prepareTopLevelBlocks(
            JsonNode dataNode,
            String defaultDatasetCode,
            String defaultControlArea,
            Map<String, Set<String>> defaultFieldToRules,
            RulesCacheSnapshot cacheSnapshot,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping
    ) {
        Map<String, JsonNode> objectBlocks = new LinkedHashMap<>();
        List<PreparedArrayItem> arrayItems = new ArrayList<>();
        Set<String> arrayLogicalFields = new LinkedHashSet<>();
        Set<String> completedControlAreas = new LinkedHashSet<>();

        if (dataNode == null || !dataNode.isObject()) {
            return new TopLevelPreparation(
                    objectBlocks,
                    arrayItems,
                    arrayLogicalFields,
                    completedControlAreas
            );
        }

        Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            JsonNode value = entry.getValue();
            if (value == null) continue;

            if (value.isObject()) {
                objectBlocks.put(entry.getKey(), value);
            } else if (value.isArray()) {
                prepareArrayBlock(
                        entry.getKey(),
                        value,
                        defaultDatasetCode,
                        defaultControlArea,
                        defaultFieldToRules,
                        cacheSnapshot,
                        genderMapping,
                        typeDulMapping,
                        completedControlAreas,
                        arrayLogicalFields,
                        arrayItems
                );
            }
        }

        return new TopLevelPreparation(
                objectBlocks,
                arrayItems,
                arrayLogicalFields,
                completedControlAreas
        );
    }

    private void prepareArrayBlock(
            String blockName,
            JsonNode arrayNode,
            String defaultDatasetCode,
            String defaultControlArea,
            Map<String, Set<String>> defaultFieldToRules,
            RulesCacheSnapshot cacheSnapshot,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping,
            Set<String> completedControlAreas,
            Set<String> arrayLogicalFields,
            List<PreparedArrayItem> arrayItems
    ) {
        if (arrayNode == null || !arrayNode.isArray()) {
            return;
        }

        int index = 0;

        for (JsonNode itemNode : arrayNode) {
            if (itemNode == null || !itemNode.isObject()) {
                index++;
                continue;
            }

            Set<String> mappedFields = collectLogicalFieldsFromBlock(itemNode);
            arrayLogicalFields.addAll(mappedFields);

            String itemDatasetCode = text(itemNode, "dataset_code", defaultDatasetCode);
            String itemControlArea = cacheSnapshot.controlAreaByDataset(itemDatasetCode);
            if (itemControlArea == null || itemControlArea.isBlank()) {
                itemControlArea = defaultControlArea;
            }

            Map<String, Set<String>> itemAllFieldToRules = cacheSnapshot.fieldToRules(itemControlArea);
            String itemRulesControlArea = itemControlArea;
            if (itemAllFieldToRules == null || itemAllFieldToRules.isEmpty()) {
                itemAllFieldToRules = defaultFieldToRules;
                itemRulesControlArea = defaultControlArea;
            }

            Boolean itemFilterFlag = cacheSnapshot.filterFlag(itemControlArea);
            Set<String> itemReferencedFields =
                    cacheSnapshot.referencedFields(itemRulesControlArea);

            // Ровно как в прежнем первом проходе: при filterFlag=true поля
            // контрольной области исключаются из main-блока один раз.
            if (Boolean.TRUE.equals(itemFilterFlag)
                    && completedControlAreas.add(itemRulesControlArea)
                    && itemAllFieldToRules != null) {
                arrayLogicalFields.addAll(itemAllFieldToRules.keySet());
            }

            Map<String, String> itemNormalized = normalizeSingleBlock(
                    blockName,
                    itemNode,
                    genderMapping,
                    typeDulMapping
            );

            Set<String> itemLogicalFields = new LinkedHashSet<>(mappedFields);
            itemLogicalFields.addAll(itemNormalized.keySet());
            if (Boolean.TRUE.equals(itemFilterFlag)) {
                itemLogicalFields.addAll(itemAllFieldToRules.keySet());
            }

            Map<String, Set<String>> itemExpectedFieldToRules = Boolean.TRUE.equals(itemFilterFlag)
                    ? itemAllFieldToRules
                    : selectKeys(itemAllFieldToRules, itemLogicalFields);

            EffectiveNormalization itemEffective = buildEffectiveNormalization(
                    itemFilterFlag,
                    itemNormalized,
                    itemExpectedFieldToRules,
                    itemReferencedFields,
                    itemLogicalFields
            );

            arrayItems.add(new PreparedArrayItem(
                    arrayInstanceKey(itemDatasetCode, blockName, index),
                    itemEffective.normalizedMap(),
                    itemEffective.fieldToRules()
            ));

            index++;
        }
    }

    private String arrayInstanceKey(String datasetCode, String blockName, int index) {
        return (datasetCode == null ? "" : datasetCode) + "#" + blockName + "#" + index;
    }

    private Map<String, String> normalizeSingleBlock(
            String blockName,
            JsonNode blockNode,
            Map<String, String> genderMapping,
            Map<String, String> typeDulMapping
    ) {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode data = mapper.createObjectNode();

        data.set(blockName, blockNode);
        root.set("data", data);

        return normalizer.normalize(root, genderMapping, typeDulMapping);
    }

    private Set<String> collectLogicalFieldsFromBlock(JsonNode blockNode) {
        Set<String> out = new LinkedHashSet<>();
        if (blockNode == null || !blockNode.isObject()) return out;

        Iterator<Map.Entry<String, JsonNode>> it = blockNode.fields();

        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            JsonNode value = e.getValue();

            if (key == null || !key.startsWith("mapping.") || value == null || value.isNull()) continue;

            String logical = isNullLikeText(value) ? null : value.asText(null);
            if (logical == null || logical.isBlank() || "none".equalsIgnoreCase(logical.trim())) continue;

            out.add(logical.trim());
        }

        return out;
    }

    private <V> Map<String, V> removeKeys(Map<String, V> source, Set<String> keysToRemove) {
        if (source == null || source.isEmpty()) return new LinkedHashMap<>();
        if (keysToRemove == null || keysToRemove.isEmpty()) return new LinkedHashMap<>(source);

        Map<String, V> result = new LinkedHashMap<>();

        for (Map.Entry<String, V> entry : source.entrySet()) {
            if (!keysToRemove.contains(entry.getKey())) result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    private <V> Map<String, V> selectKeys(Map<String, V> source, Set<String> allowedKeys) {
        Map<String, V> result = new LinkedHashMap<>();
        if (source == null || source.isEmpty() || allowedKeys == null || allowedKeys.isEmpty()) return result;

        for (Map.Entry<String, V> entry : source.entrySet()) {
            if (allowedKeys.contains(entry.getKey())) result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    private EffectiveNormalization buildEffectiveNormalization(
            Boolean filterFlag,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRules,
            Set<String> referencedFields,
            Set<String> localFields
    ) {
        // Все вызовы передают только что созданную карту нормализации.
        // Метод получает её во владение и может дополнять без полной копии.
        Map<String, String> safeNormalized = normalizedMap == null ? new LinkedHashMap<>() : normalizedMap;
        Map<String, Set<String>> safeFieldToRules = fieldToRules == null ? Map.of() : fieldToRules;

        if (Boolean.TRUE.equals(filterFlag)) {
            Map<String, Set<String>> presentFieldToRules =
                    normalizer.selectPresentFields(
                            safeNormalized,
                            safeFieldToRules
                    );

            // В локальном overlay остаются реальные значения и null только
            // для однозначно локальных полей. Межблочные зависимости будут
            // добавлены один раз в общую карту Rule.apply ниже.
            normalizer.completeMissingLocalFields(
                    safeNormalized,
                    referencedFields,
                    localFields
            );
            return new EffectiveNormalization(safeNormalized, presentFieldToRules);
        }

        Map<String, String> effective = new LinkedHashMap<>();

        if (safeFieldToRules.isEmpty()) {
            return new EffectiveNormalization(safeNormalized, safeFieldToRules);
        }

        for (String logicalField : safeFieldToRules.keySet()) {
            if (logicalField == null || logicalField.isBlank()) continue;

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

        return new EffectiveNormalization(effective, safeFieldToRules);
    }

    /**
     * Собирает единый контекст Rule.apply без дополнительной копии полной
     * нормализованной карты. Значения из исходного JSON имеют приоритет, а
     * подготовленные карты добавляют только отсутствующие ключи (обычно null).
     */
    private Map<String, String> buildCompleteRuleInput(
            Map<String, String> normalizedMap,
            EffectiveNormalization mainEffective,
            List<PreparedExcludedBlock> excludedBlocks,
            List<PreparedArrayItem> arrayItems,
            Set<String> completionControlAreas,
            RulesCacheSnapshot cacheSnapshot
    ) {
        Map<String, String> result = normalizedMap == null
                ? new LinkedHashMap<>()
                : normalizedMap;

        putAbsentEntries(result, mainEffective == null ? null : mainEffective.normalizedMap());

        if (excludedBlocks != null) {
            for (PreparedExcludedBlock block : excludedBlocks) {
                if (block != null) putAbsentEntries(result, block.normalizedMap());
            }
        }

        if (arrayItems != null) {
            for (PreparedArrayItem item : arrayItems) {
                if (item != null) putAbsentEntries(result, item.normalizedMap());
            }
        }

        // Null-дополнение выполняется только после выбора запускаемых правил
        // и только в общей карте. Поэтому синтетический ключ не запускает
        // правило отсутствующего атрибута, а реальное значение из другого
        // блока не перекрывается локальным null.
        if (completionControlAreas != null && cacheSnapshot != null) {
            for (String controlArea : completionControlAreas) {
                normalizer.completeMissingFields(
                        result,
                        cacheSnapshot.referencedFields(controlArea)
                );
            }
        }

        return result;
    }

    private void putAbsentEntries(
            Map<String, String> target,
            Map<String, String> additions
    ) {
        if (target == null || additions == null || additions.isEmpty()) return;

        for (Map.Entry<String, String> entry : additions.entrySet()) {
            // containsKey обязателен: физическое значение null не должно быть
            // заменено значением из другого блока или элемента массива.
            if (!target.containsKey(entry.getKey())) {
                target.put(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Возвращает полный контекст с локальными значениями текущего блока.
     * Полная Map не копируется: get/containsKey работают через два lookup.
     * Материализация выполняется лениво, только если конкретное Rule начнёт
     * итерировать entrySet/keySet/values.
     */
    private Map<String, String> scopedRuleInput(
            Map<String, String> completeRuleInput,
            Map<String, String> scopedValues
    ) {
        Map<String, String> base = completeRuleInput == null ? Map.of() : completeRuleInput;

        if (scopedValues == null || scopedValues.isEmpty()) {
            return base;
        }

        boolean differs = false;

        for (Map.Entry<String, String> entry : scopedValues.entrySet()) {
            String field = entry.getKey();
            if (!base.containsKey(field)
                    || !Objects.equals(base.get(field), entry.getValue())) {
                differs = true;
                break;
            }
        }

        return differs
                ? new ScopedRuleInputMap(base, scopedValues)
                : base;
    }

    private record EffectiveNormalization(
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRules
    ) {
    }

    private record PreparedExcludedBlock(
            String datasetCode,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRules
    ) {
    }

    private record TopLevelPreparation(
            Map<String, JsonNode> objectBlocks,
            List<PreparedArrayItem> arrayItems,
            Set<String> arrayLogicalFields,
            Set<String> completionControlAreas
    ) {
    }

    private record PreparedArrayItem(
            String instanceKey,
            Map<String, String> normalizedMap,
            Map<String, Set<String>> fieldToRules
    ) {
    }

    private static final class ScopedRuleInputMap
            extends AbstractMap<String, String> {

        private final Map<String, String> base;
        private final Map<String, String> scopedValues;
        private Set<Entry<String, String>> materializedEntrySet;

        private ScopedRuleInputMap(
                Map<String, String> base,
                Map<String, String> scopedValues
        ) {
            this.base = base;
            this.scopedValues = scopedValues;
        }

        @Override
        public String get(Object key) {
            return scopedValues.containsKey(key)
                    ? scopedValues.get(key)
                    : base.get(key);
        }

        @Override
        public boolean containsKey(Object key) {
            return scopedValues.containsKey(key) || base.containsKey(key);
        }

        @Override
        public int size() {
            int size = base.size();

            for (String field : scopedValues.keySet()) {
                if (!base.containsKey(field)) {
                    size++;
                }
            }

            return size;
        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            if (materializedEntrySet == null) {
                Map<String, String> materialized = new LinkedHashMap<>(base);
                materialized.putAll(scopedValues);

                materializedEntrySet = Collections.unmodifiableMap(materialized).entrySet();
            }

            return materializedEntrySet;
        }
    }

    private Map<String, Map<String, String>> safeFieldMap(Map<String, Map<String, String>> source) {
        return source == null ? Map.of() : source;
    }

    private void mergeErrorsWithPrefix(Map<String, List<String>> target, String instanceKey, Map<String, List<String>> source) {
        if (source == null || source.isEmpty()) return;

        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null || entry.getValue().isEmpty()) continue;
            target.put(instanceKey + "::" + entry.getKey(), List.copyOf(new LinkedHashSet<>(entry.getValue())));
        }
    }

    private void mergeErrors(Map<String, List<String>> target, Map<String, List<String>> source) {
        if (source == null || source.isEmpty()) return;

        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            String logicalField = entry.getKey();
            List<String> messages = entry.getValue();

            if (logicalField == null || logicalField.isBlank() || messages == null || messages.isEmpty()) continue;

            LinkedHashSet<String> merged = new LinkedHashSet<>(target.getOrDefault(logicalField, List.of()));

            for (String msg : messages) {
                if (msg != null && !msg.isBlank()) merged.add(msg);
            }

            if (!merged.isEmpty()) target.put(logicalField, new ArrayList<>(merged));
        }
    }

    private Map<String, List<String>> freezeErrors(Map<String, List<String>> source) {
        if (source == null || source.isEmpty()) return Map.of();

        Map<String, List<String>> result = new LinkedHashMap<>();

        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) continue;

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
        if (s == null) return null;

        return s
                .replaceAll("(\"birthdate\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"clientSnils\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"snils\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"number\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"series\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"departmentCode\"\\s*:\\s*\")[^\"]*(\")", "$1***$2");
    }

    private String maskJsonPretty(String json) {
        if (json == null || json.isBlank()) return json;

        try {
            JsonNode root = mapper.readTree(json);
            maskNode(root);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            return maskInline(json);
        }
    }

    private void maskNode(JsonNode node) {
        if (node == null) return;

        if (node.isObject()) {
            Iterator<String> it = node.fieldNames();

            while (it.hasNext()) {
                String fn = it.next();
                JsonNode child = node.get(fn);

                if (isSensitiveKey(fn) && node instanceof ObjectNode obj) obj.put(fn, "***");
                else maskNode(child);
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) maskNode(child);
        }
    }

    private boolean isSensitiveKey(String key) {
        if (key == null) return false;

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
        if (m == null) return Map.of();

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
        if (node == null) return def;

        JsonNode v = node.get(field);
        return v == null || v.isNull() ? def : v.asText(def);
    }

    private ProcessingResult buildProcessingResult(MessageRecord in, String shortJson, String detailJson, String originalJson) {
        if (in == null) throw new IllegalArgumentException("MessageRecord is null");

        if (in.mqMessageId != null) return ProcessingResult.forMq(in.mqMessageId, shortJson, detailJson, originalJson);

        if (in.jmsMessageId != null && !in.jmsMessageId.isBlank()) {
            return ProcessingResult.forJms(in.jmsMessageId, shortJson, detailJson, originalJson);
        }

        throw new IllegalStateException("MessageRecord has neither mqMessageId nor jmsMessageId. Cannot build ProcessingResult. " + in);
    }

    private String extractEventId(MessageRecord in) {
        if (in == null) return "unknown";
        if (in.mqMessageId != null) return MessageRecord.mqIdToHex(in.mqMessageId);
        if (in.jmsMessageId != null && !in.jmsMessageId.isBlank()) return in.jmsMessageId;
        return "unknown";
    }

    private int normalizeEmptyStringsToNull(JsonNode node) {
        if (node == null || node.isNull()) return 0;

        int converted = 0;

        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            List<String> fieldsToNull = null;

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                JsonNode child = entry.getValue();

                if (isNullLikeText(child)) {
                    if (fieldsToNull == null) {
                        fieldsToNull = new ArrayList<>();
                    }
                    fieldsToNull.add(entry.getKey());
                } else {
                    converted += normalizeEmptyStringsToNull(child);
                }
            }

            if (fieldsToNull != null) {
                for (String fieldName : fieldsToNull) {
                    objectNode.set(fieldName, mapper.nullNode());
                    converted++;
                }
            }

            return converted;
        }

        if (node.isArray()) {
            for (JsonNode child : node) converted += normalizeEmptyStringsToNull(child);
        }

        return converted;
    }

    private boolean isNullLikeText(JsonNode node) {
        if (node == null || !node.isTextual()) return false;

        String text = node.asText();
        return text == null || text.isEmpty();
    }
}
