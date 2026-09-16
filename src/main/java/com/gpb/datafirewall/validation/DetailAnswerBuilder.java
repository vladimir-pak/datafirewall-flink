package com.gpb.datafirewall.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
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
    private static final String ARRAY_KEY_SEPARATOR = "#";

    private final ObjectMapper mapper;

    public DetailAnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode buildDetailAnswer(JsonNode originalEvent, ValidationResult validation, String qid, Long createdDttm, Long readedDttm, Set<String> excludedBlocks) {
        Map<String, Map<String, String>> general = validation == null || validation.detailByField() == null ? Map.of() : validation.detailByField();
        Map<String, Map<String, Map<String, String>>> byDataset = validation == null || validation.detailByDataset() == null ? Map.of() : validation.detailByDataset();

        ObjectNode result = mapper.createObjectNode();
        String mainDataset = getText(originalEvent, "dfw_dataset_code", "UNKNOWN_DATASET");
        JsonNode data = originalEvent == null ? null : originalEvent.get("data");
        ObjectNode detailResults = mapper.createObjectNode();

        Set<String> excludedDatasetCodes = collectExcludedDatasetCodes(data, excludedBlocks);
        Set<String> excludedFromMain = new LinkedHashSet<>();

        for (String excludedDatasetCode : excludedDatasetCodes) {
            Map<String, Map<String, String>> excludedBucketMap = byDataset.getOrDefault(excludedDatasetCode, Map.of());
            excludedFromMain.addAll(excludedBucketMap.keySet());
            detailResults.set(excludedDatasetCode, buildBucket(excludedBucketMap));
        }

        for (Map.Entry<String, Map<String, Map<String, String>>> e : byDataset.entrySet()) {
            String datasetKey = e.getKey();
            if (datasetKey == null || datasetKey.isBlank() || isArrayInstanceKey(datasetKey)) continue;
            if (datasetKey.equals(mainDataset) || excludedDatasetCodes.contains(datasetKey)) continue;
            excludedFromMain.addAll(e.getValue().keySet());
        }

        Map<String, Map<String, String>> mainFiltered = removeFields(general, excludedFromMain);

        for (Map.Entry<String, Map<String, Map<String, String>>> e : byDataset.entrySet()) {
            String datasetKey = e.getKey();
            if (datasetKey == null || datasetKey.isBlank() || isArrayInstanceKey(datasetKey)) continue;
            if (excludedDatasetCodes.contains(datasetKey)) continue;
            mergeFieldStatuses(mainFiltered, e.getValue());
        }

        detailResults.set(mainDataset, buildBucket(mainFiltered));
        appendArrayResults(detailResults, byDataset);

        result.set("detail_results", detailResults);

        copyIfExists(originalEvent, result, List.of(
                "dfw_query_id",
                "dfw_hostname",
                "dfw_user_login",
                "dfw_dataset_code",
                "dfw_readed_from_mq_dttm",
                "dfw_created_dttm"
        ));

        String now = Instant.now().toString();

        result.put("dfw_action_type", "ANSWER_DETAIL");
        result.put("dfw_created_dttm", createdDttm);
        result.put("dfw_readed_dttm", readedDttm);
        result.put("dfw_action_dttm", now);
        result.put("dfw_query_id", qid);

        return result;
    }

    private void appendArrayResults(ObjectNode detailResults, Map<String, Map<String, Map<String, String>>> byDataset) {
        if (detailResults == null || byDataset == null || byDataset.isEmpty()) return;

        Map<String, List<ArrayDetailItem>> grouped = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, Map<String, String>>> entry : byDataset.entrySet()) {
            ArrayInstanceKey key = parseArrayInstanceKey(entry.getKey());
            if (key == null) continue;

            grouped.computeIfAbsent(key.blockName(), k -> new java.util.ArrayList<>())
                    .add(new ArrayDetailItem(key.datasetCode(), key.index(), entry.getValue()));
        }

        for (Map.Entry<String, List<ArrayDetailItem>> entry : grouped.entrySet()) {
            List<ArrayDetailItem> items = entry.getValue();
            items.sort(java.util.Comparator.comparingInt(ArrayDetailItem::index));

            ArrayNode array = mapper.createArrayNode();

            for (ArrayDetailItem item : items) {
                ObjectNode node = buildBucket(item.detailByField());

                if (item.datasetCode() != null && !item.datasetCode().isBlank()) {
                    node.put("dataset_code", item.datasetCode());
                }

                array.add(node);
            }

            detailResults.set(entry.getKey(), array);
        }
    }

    private boolean isArrayInstanceKey(String key) {
        return parseArrayInstanceKey(key) != null;
    }

    private ArrayInstanceKey parseArrayInstanceKey(String key) {
        if (key == null || key.isBlank()) return null;

        int lastSeparator = key.lastIndexOf(ARRAY_KEY_SEPARATOR);
        if (lastSeparator <= 0 || lastSeparator >= key.length() - 1) return null;

        int secondSeparator = key.lastIndexOf(ARRAY_KEY_SEPARATOR, lastSeparator - 1);
        if (secondSeparator < 0 || secondSeparator >= lastSeparator - 1) return null;

        String datasetCode = key.substring(0, secondSeparator);
        String blockName = key.substring(secondSeparator + 1, lastSeparator);
        String indexValue = key.substring(lastSeparator + 1);

        if (blockName.isBlank()) return null;

        try {
            int index = Integer.parseInt(indexValue);
            if (index < 0) return null;
            return new ArrayInstanceKey(datasetCode, blockName, index);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void mergeFieldStatuses(Map<String, Map<String, String>> target, Map<String, Map<String, String>> source) {
        if (source == null || source.isEmpty()) return;

        for (Map.Entry<String, Map<String, String>> fieldEntry : source.entrySet()) {
            String field = fieldEntry.getKey();
            if (field == null || field.isBlank() || fieldEntry.getValue() == null) continue;

            Map<String, String> mergedRules = new LinkedHashMap<>(target.getOrDefault(field, Map.of()));

            for (Map.Entry<String, String> ruleEntry : fieldEntry.getValue().entrySet()) {
                String rule = ruleEntry.getKey();
                String status = ruleEntry.getValue();
                if (rule == null || rule.isBlank() || status == null || status.isBlank()) continue;

                mergedRules.merge(rule, status, this::worstStatus);
            }

            target.put(field, mergedRules);
        }
    }

    private String worstStatus(String a, String b) {
        if (ERROR.equalsIgnoreCase(a) || ERROR.equalsIgnoreCase(b)) return ERROR;
        if (WARNING.equalsIgnoreCase(a) || WARNING.equalsIgnoreCase(b)) return WARNING;
        return SUCCESS;
    }

    private Map<String, Map<String, String>> removeFields(Map<String, Map<String, String>> source, Set<String> excludedFields) {
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        if (source == null || source.isEmpty()) return result;

        for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
            String logicalField = entry.getKey();
            if (logicalField == null) continue;
            if (excludedFields != null && excludedFields.contains(logicalField)) continue;

            result.put(logicalField, entry.getValue());
        }

        return result;
    }

    private Set<String> collectExcludedDatasetCodes(JsonNode data, Set<String> excludedBlocks) {
        Set<String> result = new LinkedHashSet<>();
        if (data == null || !data.isObject() || excludedBlocks == null || excludedBlocks.isEmpty()) return result;

        for (String blockName : excludedBlocks) {
            if (blockName == null || blockName.isBlank()) continue;

            JsonNode blockNode = data.get(blockName);
            if (blockNode == null || blockNode.isNull() || !blockNode.isObject()) continue;

            String datasetCode = getText(blockNode, "dataset_code", null);
            if (datasetCode != null && !datasetCode.isBlank()) result.add(datasetCode);
        }

        return result;
    }

    private ObjectNode buildBucket(Map<String, Map<String, String>> detailByField) {
        if (detailByField == null) detailByField = Map.of();

        ObjectNode bucket = mapper.createObjectNode();
        boolean hasError = false;
        boolean hasWarning = false;

        Map<String, Map<String, String>> sortedFields = detailByField instanceof TreeMap<?, ?> ? detailByField : new TreeMap<>(detailByField);

        for (Map.Entry<String, Map<String, String>> fieldEntry : sortedFields.entrySet()) {
            String logicalField = fieldEntry.getKey();
            Map<String, String> ruleMap = fieldEntry.getValue();

            if (logicalField == null || logicalField.isBlank() || ruleMap == null || ruleMap.isEmpty()) continue;

            ObjectNode rulesNode = mapper.createObjectNode();
            Map<String, String> sortedRules = ruleMap instanceof TreeMap<?, ?> ? ruleMap : new TreeMap<>(ruleMap);
            boolean fieldHasResult = false;

            for (Map.Entry<String, String> ruleEntry : sortedRules.entrySet()) {
                String ruleName = ruleEntry.getKey();
                String status = ruleEntry.getValue();

                if (ruleName == null || ruleName.isBlank() || status == null || status.isBlank()) continue;

                String normRuleKey = normalizeRuleKey(ruleName);
                rulesNode.put(normRuleKey, status);
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
        Matcher m = RULE_NUM.matcher(ruleName.trim());
        if (m.matches()) return m.group(1);
        return ruleName.trim();
    }

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        if (src == null || dst == null || fields == null) return;

        for (String f : fields) {
            if (f == null) continue;

            JsonNode v = src.get(f);
            if (v != null && !v.isNull()) dst.set(f, v);
        }
    }

    private static String getText(JsonNode node, String field, String def) {
        if (node == null) return def;

        JsonNode v = node.get(field);
        return v == null || v.isNull() ? def : v.asText(def);
    }

    private record ArrayInstanceKey(String datasetCode, String blockName, int index) {}

    private record ArrayDetailItem(String datasetCode, int index, Map<String, Map<String, String>> detailByField) {}
}