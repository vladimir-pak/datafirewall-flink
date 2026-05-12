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

    private final ObjectMapper mapper;

    public DetailAnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode buildDetailAnswer(JsonNode originalEvent, ValidationResult validation) {
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

        JsonNode homeAddressNode = data == null ? null : firstExisting(data, "homeAddress");
        JsonNode registrationAddressNode = data == null ? null : firstExisting(data, "registrationAddress", "addressRegistration");

        String homeDs = getText(homeAddressNode, "dataset_code", "УС.ЛиК.Адрес проживания");
        String regDs  = getText(registrationAddressNode, "dataset_code", "УС.ЛиК.Адрес регистрации");

        ObjectNode detailResults = mapper.createObjectNode();

        Map<String, Map<String, String>> homeBucketMap = byDataset.getOrDefault(homeDs, Map.of());
        Map<String, Map<String, String>> regBucketMap = byDataset.getOrDefault(regDs, Map.of());

        Set<String> excludedFromMain = new LinkedHashSet<>();
        excludedFromMain.addAll(homeBucketMap.keySet());
        excludedFromMain.addAll(regBucketMap.keySet());

        for (Map.Entry<String, Map<String, Map<String, String>>> e : byDataset.entrySet()) {
            String datasetCode = e.getKey();
            if (datasetCode == null || datasetCode.isBlank()) {
                continue;
            }
            if (datasetCode.equals(mainDataset)) {
                continue;
            }
            excludedFromMain.addAll(e.getValue().keySet());
        }

        Map<String, Map<String, String>> mainFiltered = removeFields(general, excludedFromMain);
        detailResults.set(mainDataset, buildBucket(mainFiltered));

        if (byDataset.containsKey(homeDs)) {
            detailResults.set(homeDs, buildBucket(homeBucketMap));
        } else {
            ObjectNode emptyHome = mapper.createObjectNode();
            emptyHome.put("ALL_RESULT", "SUCCESS");
            detailResults.set(homeDs, emptyHome);
        }

        if (byDataset.containsKey(regDs)) {
            detailResults.set(regDs, buildBucket(regBucketMap));
        } else {
            ObjectNode emptyReg = mapper.createObjectNode();
            emptyReg.put("ALL_RESULT", "SUCCESS");
            detailResults.set(regDs, emptyReg);
        }

        for (Map.Entry<String, Map<String, Map<String, String>>> e : byDataset.entrySet()) {
            String datasetCode = e.getKey();
            if (datasetCode == null || datasetCode.isBlank()) {
                continue;
            }
            if (datasetCode.equals(homeDs) || datasetCode.equals(regDs)) {
                continue;
            }
            detailResults.set(datasetCode, buildBucket(e.getValue()));
        }

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
        result.put("dfw_created_dttm", originalEvent.get("createdDttm").toString());
        result.put("dfw_readed_dttm", originalEvent.get("readedDttm").toString());
        result.put("dfw_action_dttm", now);
        result.put("dfw_query_id", originalEvent.get("eventId").toString());
        return result;
    }

    private Map<String, Map<String, String>> removeFields(
            Map<String, Map<String, String>> source,
            Set<String> excludedFields
    ) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        if (excludedFields == null || excludedFields.isEmpty()) {
            return source;
        }

        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
            String logicalField = entry.getKey();
            if (logicalField == null || excludedFields.contains(logicalField)) {
                continue;
            }
            result.put(logicalField, entry.getValue());
        }
        return result;
    }

    private ObjectNode buildBucket(Map<String, Map<String, String>> detailByField) {
        if (detailByField == null) {
            detailByField = Map.of();
        }

        ObjectNode bucket = mapper.createObjectNode();
        boolean hasError = false;

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

            boolean fieldHasError = false;
            for (Map.Entry<String, String> ruleEntry : sortedRules.entrySet()) {
                String ruleName = ruleEntry.getKey();
                String status = ruleEntry.getValue();
                if (ruleName == null || ruleName.isBlank() || status == null) {
                    continue;
                }

                String normRuleKey = normalizeRuleKey(ruleName);
                rulesNode.put(normRuleKey, status);

                if ("ERROR".equalsIgnoreCase(status)) {
                    fieldHasError = true;
                }
            }

            if (fieldHasError) {
                hasError = true;
            }

            bucket.set(logicalField, rulesNode);
        }

        bucket.put("ALL_RESULT", hasError ? "ERROR" : "SUCCESS");
        return bucket;
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

    private static JsonNode firstExisting(JsonNode node, String... names) {
        if (node == null || names == null) {
            return null;
        }
        for (String name : names) {
            if (name == null) {
                continue;
            }
            JsonNode found = node.get(name);
            if (found != null && !found.isNull()) {
                return found;
            }
        }
        return null;
    }
}