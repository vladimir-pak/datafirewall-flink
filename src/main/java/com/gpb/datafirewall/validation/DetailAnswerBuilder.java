package com.gpb.datafirewall.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        Set<String> excludedBlocks
    ) {
        Map<String, Map<String, String>> general =
                validation == null || validation.detailByField() == null
                        ? Map.of()
                        : validation.detailByField();

        Map<String, Map<String, Map<String, String>>> byDataset =
                validation == null || validation.detailByDataset() == null
                        ? Map.of()
                        : validation.detailByDataset();

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
            String datasetCode = e.getKey();
            if (datasetCode == null || datasetCode.isBlank()) {
                continue;
            }
            if (datasetCode.equals(mainDataset) || excludedDatasetCodes.contains(datasetCode)) {
                continue;
            }
            excludedFromMain.addAll(e.getValue().keySet());
        }

        Map<String, Map<String, String>> mainFiltered = removeFields(general, excludedFromMain);

        for (Map.Entry<String, Map<String, Map<String, String>>> e : byDataset.entrySet()) {
            String datasetCode = e.getKey();
            if (datasetCode == null || datasetCode.isBlank()) {
                continue;
            }
            if (excludedDatasetCodes.contains(datasetCode)) {
                continue;
            }
            mainFiltered.putAll(e.getValue());
        }

        detailResults.set(mainDataset, buildBucket(mainFiltered));

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
        // result.put("dfw_action_dttm", now);
        // if (result.get("dfw_created_dttm") == null) {
        //     result.put("dfw_created_dttm", now);
        // }
        result.put("dfw_created_dttm", createdDttm);
        result.put("dfw_readed_dttm", readedDttm);
        result.put("dfw_action_dttm", now);
        result.put("dfw_query_id", qid);
        return result;
    }

    private Map<String, Map<String, String>> removeFields(
            Map<String, Map<String, String>> source,
            Set<String> excludedFields
    ) {
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        if (source == null || source.isEmpty()) {
            return result;
        }

        for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
            String logicalField = entry.getKey();
            if (logicalField == null) {
                continue;
            }
            if (excludedFields != null && excludedFields.contains(logicalField)) {
                continue;
            }
            result.put(logicalField, entry.getValue());
        }
        return result;
    }

    private Set<String> collectExcludedDatasetCodes(JsonNode data, Set<String> excludedBlocks) {
        Set<String> result = new LinkedHashSet<>();
        if (data == null || !data.isObject() || excludedBlocks == null || excludedBlocks.isEmpty()) {
            return result;
        }

        for (String blockName : excludedBlocks) {
            if (blockName == null || blockName.isBlank()) {
                continue;
            }

            JsonNode blockNode = data.get(blockName);
            if (blockNode == null || blockNode.isNull() || !blockNode.isObject()) {
                continue;
            }

            String datasetCode = getText(blockNode, "dataset_code", null);
            if (datasetCode != null && !datasetCode.isBlank()) {
                result.add(datasetCode);
            }
        }

        return result;
    }

    private ObjectNode buildBucket(Map<String, Map<String, String>> detailByField) {
        if (detailByField == null) {
            detailByField = Map.of();
        }

        ObjectNode bucket = mapper.createObjectNode();

        boolean hasError = false;
        boolean hasWarning = false;

        Map<String, Map<String, String>> sortedFields =
                (detailByField instanceof TreeMap<?, ?>)
                        ? detailByField
                        : new TreeMap<>(detailByField);

        for (Map.Entry<String, Map<String, String>> fieldEntry : sortedFields.entrySet()) {
            String logicalField = fieldEntry.getKey();
            Map<String, String> ruleMap = fieldEntry.getValue();

            if (logicalField == null || logicalField.isBlank() || ruleMap == null || ruleMap.isEmpty()) {
                continue;
            }

            ObjectNode rulesNode = mapper.createObjectNode();

            Map<String, String> sortedRules =
                    (ruleMap instanceof TreeMap<?, ?>)
                            ? ruleMap
                            : new TreeMap<>(ruleMap);

            boolean fieldHasResult = false;

            for (Map.Entry<String, String> ruleEntry : sortedRules.entrySet()) {
                String ruleName = ruleEntry.getKey();
                String status = ruleEntry.getValue();

                if (ruleName == null || ruleName.isBlank() || status == null || status.isBlank()) {
                    continue;
                }

                String normRuleKey = normalizeRuleKey(ruleName);
                rulesNode.put(normRuleKey, status);

                fieldHasResult = true;

                if (ERROR.equalsIgnoreCase(status)) {
                    hasError = true;
                } else if (WARNING.equalsIgnoreCase(status)) {
                    hasWarning = true;
                }
            }

            if (fieldHasResult) {
                bucket.set(logicalField, rulesNode);
            }
        }

        bucket.put("ALL_RESULT", resolveAllResult(hasError, hasWarning));
        return bucket;
    }

    private static String resolveAllResult(boolean hasError, boolean hasWarning) {
        if (hasError) {
            return ERROR;
        }
        if (hasWarning) {
            return WARNING;
        }
        return SUCCESS;
    }

    private static String normalizeRuleKey(String ruleName) {
        Matcher m = RULE_NUM.matcher(ruleName.trim());
        if (m.matches()) {
            return m.group(1);
        }
        return ruleName.trim();
    }

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        if (src == null || dst == null || fields == null) {
            return;
        }
        for (String f : fields) {
            if (f == null) {
                continue;
            }
            JsonNode v = src.get(f);
            if (v != null && !v.isNull()) {
                dst.set(f, v);
            }
        }
    }

    private static String getText(JsonNode node, String field, String def) {
        if (node == null) {
            return def;
        }
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? def : v.asText(def);
    }

}