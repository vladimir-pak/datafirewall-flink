package com.gpb.datafirewall.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.util.DfwDateTimeFormatter;
import com.gpb.datafirewall.util.HostnameUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DetailAnswerBuilder {

    private static final Pattern RULE_NUM = Pattern.compile("(?i)^Rule(\\d+)$");
    private static final String ERROR = "ERROR";
    private static final String WARNING = "WARNING";
    private static final String SUCCESS = "SUCCESS";
    private static final String INSTANCE_SEPARATOR = "#";
    private static final String DATASET_CODE = "dataset_code";
    private static final String KEY = "$key";

    private final ObjectMapper mapper;

    public DetailAnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode buildDetailAnswer(
            JsonNode originalEvent, 
            ValidationResult validation, 
            String qid, 
            Long createdDttm, 
            Long readedDttm, 
            Set<String> excludedBlocks) {
                
        Map<String, Map<String, String>> general = 
                validation == null || validation.detailByField() == null 
                        ? Map.of() : validation.detailByField();

        Map<String, Map<String, Map<String, String>>> byDataset = 
                validation == null || validation.detailByDataset() == null 
                        ? Map.of() : validation.detailByDataset();

        ObjectNode result = mapper.createObjectNode();

        String mainDataset = getText(
                originalEvent, 
                "dfw_dataset_code", 
                "UNKNOWN_DATASET"
        );

        JsonNode data = originalEvent == null ? null : originalEvent.get("data");

        ObjectNode detailResults = mapper.createObjectNode();

        Set<String> excludedDatasetCodes = 
                collectExcludedDatasetCodes(data, excludedBlocks);

        Set<String> excludedFromMain = new LinkedHashSet<>();

        for (String excludedDatasetCode : excludedDatasetCodes) {
            Map<String, Map<String, String>> excludedBucketMap = 
                    byDataset.getOrDefault(excludedDatasetCode, Map.of());

            excludedFromMain.addAll(excludedBucketMap.keySet());
            
            detailResults.set(
                    excludedDatasetCode, 
                    buildBucket(excludedBucketMap)
            );
        }

        for (Map.Entry<String, Map<String, Map<String, String>>> entry : byDataset.entrySet()) {
            String key = entry.getKey();

            if (
                key == null 
                || key.isBlank() 
                || key.equals(mainDataset) 
                || excludedDatasetCodes.contains(key) 
                || isArrayInstanceKey(key)
            ) continue;

            Map<String, Map<String, String>> fields = entry.getValue();

            if (fields != null) excludedFromMain.addAll(fields.keySet());
        }

        Map<String, Map<String, String>> mainFiltered = removeFields(general, excludedFromMain);

        for (Map.Entry<String, Map<String, Map<String, String>>> entry : byDataset.entrySet()) {
            String key = entry.getKey();

            if (
                key == null 
                || key.isBlank() 
                || excludedDatasetCodes.contains(key) 
                || isArrayInstanceKey(key)
            ) continue;

            Map<String, Map<String, String>> fields = entry.getValue();

            if (fields != null && !fields.isEmpty()) mergeFieldResults(mainFiltered, fields);
        }

        detailResults.set(mainDataset, buildBucket(mainFiltered));
        appendArrayResults(detailResults, data, byDataset);

        result.set("detail_results", detailResults);

        copyIfExists(originalEvent, result, List.of(
                "dfw_query_id",
                "dfw_hostname",
                "dfw_user_login",
                "dfw_dataset_code",
                "dfw_readed_from_mq_dttm",
                "dfw_created_dttm"
        ));

        result.put("dfw_action_type", "ANSWER_DETAIL");
        result.put(
                "dfw_created_dttm", 
                DfwDateTimeFormatter.formatEpochMillis(createdDttm)
        );
        result.put(
                "dfw_readed_dttm", 
                DfwDateTimeFormatter.formatEpochMillis(readedDttm)
        );
        result.put(
                "dfw_hostname", 
                HostnameUtils.getHostname()
        );

        result.put("dfw_action_dttm", Instant.now().toString());
        result.put("dfw_query_id", qid);

        return result;
    }

    private void appendArrayResults(
            ObjectNode detailResults, 
            JsonNode data, 
            Map<String, Map<String, Map<String, String>>> byDataset) {

        if (detailResults == null || byDataset == null || byDataset.isEmpty()) return;

        Map<String, List<ArrayDetailItem>> grouped = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, Map<String, String>>> entry : byDataset.entrySet()) {
            ArrayInstanceKey instanceKey = parseArrayInstanceKey(entry.getKey());
            if (instanceKey == null) continue;

            grouped.computeIfAbsent(instanceKey.blockName(), k -> new ArrayList<>())
                    .add(
                            new ArrayDetailItem(
                                    instanceKey.datasetCode(), 
                                    instanceKey.blockName(), 
                                    instanceKey.index(), 
                                    entry.getValue()
                            )
                    );
        }

        for (Map.Entry<String, List<ArrayDetailItem>> entry : grouped.entrySet()) {
            String blockName = entry.getKey();
            List<ArrayDetailItem> items = entry.getValue();
            items.sort(java.util.Comparator.comparingInt(ArrayDetailItem::index));

            ArrayNode array = mapper.createArrayNode();

            for (ArrayDetailItem item : items) {
                ObjectNode node = buildBucket(item.detailByField());

                JsonNode sourceItem = findArrayItem(data, item.blockName(), item.index());

                if (sourceItem != null) {
                    copyTechnicalKey(sourceItem, node);

                    String sourceDatasetCode = getText(sourceItem, DATASET_CODE, item.datasetCode());
                    if (sourceDatasetCode != null && !sourceDatasetCode.isBlank()) node.put(DATASET_CODE, sourceDatasetCode);
                } else if (item.datasetCode() != null && !item.datasetCode().isBlank()) {
                    node.put(DATASET_CODE, item.datasetCode());
                }

                array.add(node);
            }

            detailResults.set(blockName, array);
        }
    }

    private JsonNode findArrayItem(JsonNode data, String blockName, int index) {
        if (data == null || !data.isObject() || blockName == null || index < 0) return null;

        JsonNode array = data.get(blockName);
        if (array == null || !array.isArray() || index >= array.size()) return null;

        JsonNode item = array.get(index);
        return item != null && item.isObject() ? item : null;
    }

    private ArrayInstanceKey parseArrayInstanceKey(String key) {
        if (key == null || key.isBlank()) return null;

        int second = key.lastIndexOf(INSTANCE_SEPARATOR);
        if (second <= 0 || second == key.length() - 1) return null;

        int first = key.lastIndexOf(INSTANCE_SEPARATOR, second - 1);
        if (first < 0 || first == second - 1) return null;

        String datasetCode = key.substring(0, first);
        String blockName = key.substring(first + 1, second);
        String indexPart = key.substring(second + 1);

        try {
            int index = Integer.parseInt(indexPart);
            return new ArrayInstanceKey(datasetCode, blockName, index);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private boolean isArrayInstanceKey(String key) {
        return parseArrayInstanceKey(key) != null;
    }

    private void copyTechnicalKey(JsonNode source, ObjectNode target) {
        if (source == null || target == null) return;

        JsonNode keyNode = source.get(KEY);
        if (keyNode != null && !keyNode.isNull()) target.set(KEY, keyNode.deepCopy());
    }

    private void mergeFieldResults(
            Map<String, Map<String, String>> target, 
            Map<String, Map<String, String>> source) {

        if (target == null || source == null || source.isEmpty()) return;

        for (Map.Entry<String, Map<String, String>> fieldEntry : source.entrySet()) {
            String logicalField = fieldEntry.getKey();
            Map<String, String> sourceRules = fieldEntry.getValue();

            if (logicalField == null || logicalField.isBlank() || sourceRules == null || sourceRules.isEmpty()) continue;

            Map<String, String> targetRules = target.computeIfAbsent(logicalField, key -> new LinkedHashMap<>());

            for (Map.Entry<String, String> ruleEntry : sourceRules.entrySet()) {
                String ruleName = ruleEntry.getKey();
                String newStatus = ruleEntry.getValue();

                if (ruleName == null || ruleName.isBlank() || newStatus == null || newStatus.isBlank()) continue;

                String oldStatus = targetRules.get(ruleName);
                targetRules.put(ruleName, strongestStatus(oldStatus, newStatus));
            }
        }
    }

    private String strongestStatus(String current, String candidate) {
        if (current == null || current.isBlank()) return candidate;
        if (candidate == null || candidate.isBlank()) return current;
        if (ERROR.equalsIgnoreCase(current) || ERROR.equalsIgnoreCase(candidate)) return ERROR;
        if (WARNING.equalsIgnoreCase(current) || WARNING.equalsIgnoreCase(candidate)) return WARNING;
        return SUCCESS;
    }

    private Map<String, Map<String, String>> removeFields(
            Map<String, Map<String, String>> source, 
            Set<String> excludedFields) {

        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        if (source == null || source.isEmpty()) return result;

        for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
            String logicalField = entry.getKey();

            if (logicalField == null) continue;
            if (excludedFields != null && excludedFields.contains(logicalField)) continue;

            Map<String, String> rules = entry.getValue();
            result.put(logicalField, rules == null ? new LinkedHashMap<>() : new LinkedHashMap<>(rules));
        }

        return result;
    }

    private Set<String> collectExcludedDatasetCodes(
            JsonNode data, 
            Set<String> excludedBlocks) {

        Set<String> result = new LinkedHashSet<>();

        if (data == null || !data.isObject() || excludedBlocks == null || excludedBlocks.isEmpty()) return result;

        for (String blockName : excludedBlocks) {
            if (blockName == null || blockName.isBlank()) continue;

            JsonNode blockNode = data.get(blockName);
            if (blockNode == null || blockNode.isNull()) continue;

            if (blockNode.isObject()) {
                String datasetCode = getText(blockNode, DATASET_CODE, null);
                if (datasetCode != null && !datasetCode.isBlank()) result.add(datasetCode);
                continue;
            }

            if (blockNode.isArray()) {
                for (JsonNode item : blockNode) {
                    if (item == null || !item.isObject()) continue;

                    String datasetCode = getText(item, DATASET_CODE, null);
                    if (datasetCode != null && !datasetCode.isBlank()) result.add(datasetCode);
                }
            }
        }

        return result;
    }

    private ObjectNode buildBucket(Map<String, Map<String, String>> detailByField) {
        if (detailByField == null) detailByField = Map.of();

        ObjectNode bucket = mapper.createObjectNode();
        boolean hasError = false;
        boolean hasWarning = false;

        Map<String, Map<String, String>> sortedFields = 
                detailByField instanceof TreeMap<?, ?> 
                        ? detailByField : new TreeMap<>(detailByField);

        for (Map.Entry<String, Map<String, String>> fieldEntry : sortedFields.entrySet()) {
            String logicalField = fieldEntry.getKey();
            Map<String, String> ruleMap = fieldEntry.getValue();

            if (logicalField == null || logicalField.isBlank() || ruleMap == null || ruleMap.isEmpty()) continue;

            ObjectNode rulesNode = mapper.createObjectNode();
            Map<String, String> sortedRules = 
                    ruleMap instanceof TreeMap<?, ?> 
                            ? ruleMap : new TreeMap<>(ruleMap);
            boolean fieldHasResult = false;

            for (Map.Entry<String, String> ruleEntry : sortedRules.entrySet()) {
                String ruleName = ruleEntry.getKey();
                String status = ruleEntry.getValue();

                if (ruleName == null || ruleName.isBlank() || status == null || status.isBlank()) continue;

                rulesNode.put(normalizeRuleKey(ruleName), status);
                fieldHasResult = true;

                if (ERROR.equalsIgnoreCase(status)) hasError = true;
                else if (WARNING.equalsIgnoreCase(status)) hasWarning = true;
            }

            if (fieldHasResult) bucket.set(logicalField, rulesNode);
        }

        bucket.put("ALL_RESULT", resolveAllResult(hasError, hasWarning));
        return bucket;
    }

    private static String resolveAllResult(boolean hasError, boolean hasWarning) {
        if (hasError) return ERROR;
        if (hasWarning) return WARNING;
        return SUCCESS;
    }

    private static String normalizeRuleKey(String ruleName) {
        Matcher matcher = RULE_NUM.matcher(ruleName.trim());
        return matcher.matches() ? matcher.group(1) : ruleName.trim();
    }

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        if (src == null || dst == null || fields == null) return;

        for (String field : fields) {
            if (field == null) continue;

            JsonNode value = src.get(field);
            if (value != null && !value.isNull()) dst.set(field, value);
        }
    }

    private static String getText(JsonNode node, String field, String def) {
        if (node == null) return def;

        JsonNode value = node.get(field);
        return value == null || value.isNull() ? def : value.asText(def);
    }

    private record ArrayInstanceKey(
            String datasetCode, 
            String blockName, 
            int index) {}

    private record ArrayDetailItem(
            String datasetCode, 
            String blockName, 
            int index, 
            Map<String, Map<String, String>> detailByField) {}
}