package com.gpb.datafirewall.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class AnswerBuilder {

    private static final String MAPPING_PREFIX = "mapping.";
    private static final String DATASET_CODE = "dataset_code";
    private static final String KEY = "$key";
    private static final String ELEM_ID = "elemId";

    private final ObjectMapper mapper;

    public AnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode buildAnswer(JsonNode originalEvent, ValidationResult validation, String qid, Long createdDttm, Long readedDttm) {
        ObjectNode out = mapper.createObjectNode();

        copyIfExists(originalEvent, out, List.of(
                "dfw_query_id",
                "dfw_hostname",
                "dfw_user_login",
                "dfw_dataset_code",
                "dfw_readed_from_mq_dttm",
                "dfw_created_dttm"
        ));

        out.put("dfw_action_type", "ANSWER");
        out.put("dfw_created_dttm", createdDttm);
        out.put("dfw_readed_dttm", readedDttm);
        out.put("dfw_action_dttm", Instant.now().toString());
        out.put("dfw_query_id", qid);

        String processStatus = validation == null || validation.processStatus() == null ? "ERROR" : validation.processStatus();
        out.put("PROCESS_STATUS", processStatus);

        ObjectNode details = buildShortDetails(originalEvent, validation);
        details.set("errors", buildErrors(originalEvent, validation));
        out.set("details", details);

        ObjectNode wrapped = mapper.createObjectNode();
        wrapped.set("data", out);
        return wrapped;
    }

    private ObjectNode buildShortDetails(JsonNode originalEvent, ValidationResult validation) {
        ObjectNode details = mapper.createObjectNode();

        Map<String, Map<String, String>> general = validation == null || validation.detailByField() == null ? Map.of() : validation.detailByField();
        Map<String, Map<String, Map<String, String>>> byDataset = validation == null || validation.detailByDataset() == null ? Map.of() : validation.detailByDataset();

        JsonNode data = originalEvent == null ? null : originalEvent.get("data");
        String allResult = validation == null || validation.allResult() == null ? "ERROR" : validation.allResult();

        details.put("ALL_RESULT", allResult);
        appendDynamicShortDetails(details, data, general, byDataset);
        return details;
    }

    private ObjectNode buildErrors(JsonNode originalEvent, ValidationResult validation) {
        ObjectNode errors = mapper.createObjectNode();
        Map<String, List<String>> errorsByField = validation == null || validation.errorsByField() == null ? Map.of() : validation.errorsByField();

        if (errorsByField.isEmpty()) return errors;

        JsonNode data = originalEvent == null ? null : originalEvent.get("data");
        if (data == null || !data.isObject()) return errors;

        Iterator<Map.Entry<String, JsonNode>> blocks = data.fields();

        while (blocks.hasNext()) {
            Map.Entry<String, JsonNode> blockEntry = blocks.next();
            String blockName = blockEntry.getKey();
            JsonNode blockNode = blockEntry.getValue();

            if (blockName == null || blockNode == null) continue;

            if (blockNode.isArray()) {
                ArrayNode arrayErrors = buildTopLevelArrayErrors(blockNode, blockName, errorsByField);
                if (arrayErrors.size() > 0) errors.set(blockName, arrayErrors);
                continue;
            }

            if (!blockNode.isObject()) continue;

            ObjectNode blockErrors = buildBlockErrors(blockNode, errorsByField, null);

            if ("documents".equals(blockName)) {
                ArrayNode cardsErrors = buildClientIdCardErrors(blockNode, errorsByField);
                if (cardsErrors.size() > 0) blockErrors.set("clientIdCard", cardsErrors);
            }

            if (blockErrors.size() > 0) errors.set(blockName, blockErrors);
        }

        return errors;
    }

    private ArrayNode buildTopLevelArrayErrors(JsonNode arrayNode, String blockName, Map<String, List<String>> errorsByField) {
        ArrayNode result = mapper.createArrayNode();
        if (arrayNode == null || !arrayNode.isArray()) return result;

        int index = 0;

        for (JsonNode item : arrayNode) {
            if (item == null || !item.isObject()) {
                index++;
                continue;
            }

            String datasetCode = text(item, DATASET_CODE, null);
            String instanceKey = arrayInstanceKey(datasetCode, blockName, index);

            ObjectNode itemErrors = buildBlockErrors(item, errorsByField, instanceKey);
            copyTechnicalKey(item, itemErrors);

            result.add(itemErrors);
            index++;
        }

        return result;
    }

    private ObjectNode buildBlockErrors(JsonNode blockNode, Map<String, List<String>> errorsByField, String instanceKey) {
        ObjectNode blockErrors = mapper.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = blockNode.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();

            if (key == null || !key.startsWith(MAPPING_PREFIX) || value == null || value.isNull()) continue;

            String localFieldName = key.substring(MAPPING_PREFIX.length()).trim();
            String logicalField = value.asText(null);

            if (logicalField == null || logicalField.isBlank() || "none".equalsIgnoreCase(logicalField)) continue;

            List<String> fieldErrors = findErrorsByLogicalField(errorsByField, logicalField, instanceKey);
            if (fieldErrors == null || fieldErrors.isEmpty()) continue;

            ArrayNode arr = mapper.createArrayNode();
            for (String message : fieldErrors) {
                if (message != null) arr.add(message);
            }

            if (arr.size() > 0) blockErrors.set(localFieldName, arr);
        }

        return blockErrors;
    }

    private ArrayNode buildClientIdCardErrors(JsonNode documentsNode, Map<String, List<String>> errorsByField) {
        ArrayNode result = mapper.createArrayNode();
        JsonNode cards = documentsNode == null ? null : documentsNode.get("clientIdCard");

        if (cards == null || !cards.isArray() || cards.isEmpty()) return result;

        for (JsonNode cardNode : cards) {
            if (cardNode == null || !cardNode.isObject()) continue;

            ObjectNode cardErrors = buildBlockErrors(cardNode, errorsByField, null);
            if (cardErrors.size() > 0) result.add(cardErrors);
        }

        return result;
    }

    private void appendDynamicShortDetails(ObjectNode details, JsonNode data, Map<String, Map<String, String>> general, Map<String, Map<String, Map<String, String>>> byDataset) {
        if (details == null || data == null || !data.isObject()) return;

        Iterator<Map.Entry<String, JsonNode>> blocks = data.fields();

        while (blocks.hasNext()) {
            Map.Entry<String, JsonNode> blockEntry = blocks.next();
            String blockName = blockEntry.getKey();
            JsonNode blockNode = blockEntry.getValue();

            if (blockName == null || blockNode == null) continue;

            if (blockNode.isArray()) {
                ArrayNode blockShortArray = buildTopLevelShortArray(blockNode, blockName, general, byDataset);
                if (blockShortArray.size() > 0) details.set(blockName, blockShortArray);
                continue;
            }

            if (!blockNode.isObject()) continue;

            ObjectNode blockShortNode = buildGenericShortNode(blockNode, null, general, byDataset);
            if (blockShortNode.size() > 0) details.set(blockName, blockShortNode);
        }
    }

    private ArrayNode buildTopLevelShortArray(JsonNode arrayNode, String blockName, Map<String, Map<String, String>> general, Map<String, Map<String, Map<String, String>>> byDataset) {
        ArrayNode result = mapper.createArrayNode();
        if (arrayNode == null || !arrayNode.isArray()) return result;

        int index = 0;

        for (JsonNode item : arrayNode) {
            if (item == null || !item.isObject()) {
                index++;
                continue;
            }

            String datasetCode = text(item, DATASET_CODE, null);
            String instanceKey = arrayInstanceKey(datasetCode, blockName, index);

            ObjectNode child = buildGenericShortNode(item, instanceKey, general, byDataset);

            copyTechnicalKey(item, child);

            if (datasetCode != null && !child.has(DATASET_CODE)) child.put(DATASET_CODE, datasetCode);

            result.add(child);
            index++;
        }

        return result;
    }

    private ObjectNode buildGenericShortNode(JsonNode sourceNode, String instanceKey, Map<String, Map<String, String>> general, Map<String, Map<String, Map<String, String>>> byDataset) {
        ObjectNode result = mapper.createObjectNode();
        if (sourceNode == null || !sourceNode.isObject()) return result;

        String datasetCode = text(sourceNode, DATASET_CODE, null);

        appendMappedFieldStatuses(result, sourceNode, datasetCode, instanceKey, general, byDataset);
        appendNestedShortNodes(result, sourceNode, general, byDataset);

        if (result.size() == 0) return result;
        if (datasetCode == null) return result;

        ObjectNode withDatasetCode = mapper.createObjectNode();
        withDatasetCode.put(DATASET_CODE, datasetCode);
        withDatasetCode.setAll(result);
        return withDatasetCode;
    }

    private void appendMappedFieldStatuses(ObjectNode result, JsonNode sourceNode, String datasetCode, String instanceKey, Map<String, Map<String, String>> general, Map<String, Map<String, Map<String, String>>> byDataset) {
        Iterator<Map.Entry<String, JsonNode>> fields = sourceNode.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> fieldEntry = fields.next();
            String key = fieldEntry.getKey();

            if (key == null || !key.startsWith(MAPPING_PREFIX)) continue;

            String localFieldName = key.substring(MAPPING_PREFIX.length()).trim();
            if (localFieldName.isEmpty()) continue;

            String status = statusByMappingFlexibleOrNull(sourceNode, key, datasetCode, instanceKey, general, byDataset);
            if (status != null) result.put(localFieldName, status);
        }
    }

    private void appendNestedShortNodes(ObjectNode result, JsonNode sourceNode, Map<String, Map<String, String>> general, Map<String, Map<String, Map<String, String>>> byDataset) {
        Iterator<Map.Entry<String, JsonNode>> fields = sourceNode.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> fieldEntry = fields.next();
            String key = fieldEntry.getKey();
            JsonNode value = fieldEntry.getValue();

            if (key == null || key.startsWith(MAPPING_PREFIX) || DATASET_CODE.equals(key) || KEY.equals(key)) continue;
            if (value == null || value.isNull()) continue;

            if (value.isObject()) {
                ObjectNode child = buildGenericShortNode(value, null, general, byDataset);
                if (child.size() > 0) result.set(key, child);
                continue;
            }

            if (value.isArray()) {
                ArrayNode children = buildGenericShortArray(value, general, byDataset);
                if (children.size() > 0) result.set(key, children);
            }
        }
    }

    private ArrayNode buildGenericShortArray(JsonNode arrayNode, Map<String, Map<String, String>> general, Map<String, Map<String, Map<String, String>>> byDataset) {
        ArrayNode result = mapper.createArrayNode();
        if (arrayNode == null || !arrayNode.isArray()) return result;

        for (JsonNode item : arrayNode) {
            if (item == null || !item.isObject()) continue;

            ObjectNode child = buildGenericShortNode(item, null, general, byDataset);
            if (child.size() == 0) continue;

            String elemId = text(item, ELEM_ID, null);
            if (elemId != null && !child.has(ELEM_ID)) child.put(ELEM_ID, elemId);

            result.add(child);
        }

        return result;
    }

    private List<String> findErrorsByLogicalField(Map<String, List<String>> errorsByField, String logicalField, String instanceKey) {
        if (logicalField == null || logicalField.isBlank() || errorsByField == null || errorsByField.isEmpty()) return null;

        if (instanceKey != null) {
            List<String> instanceErrors = errorsByField.get(instanceKey + "::" + logicalField);
            if (instanceErrors != null && !instanceErrors.isEmpty()) return instanceErrors;

            String alt = logicalField.replace('.', ',');
            instanceErrors = errorsByField.get(instanceKey + "::" + alt);
            if (instanceErrors != null && !instanceErrors.isEmpty()) return instanceErrors;
        }

        List<String> errors = errorsByField.get(logicalField);
        if (errors != null && !errors.isEmpty()) return errors;

        errors = errorsByField.get(logicalField.replace('.', ','));
        return errors != null && !errors.isEmpty() ? errors : null;
    }

    private String statusByMappingFlexibleOrNull(JsonNode node, String mappingKey, String datasetCode, String instanceKey, Map<String, Map<String, String>> general, Map<String, Map<String, Map<String, String>>> byDataset) {
        Map<String, String> rules = findRulesByMapping(node, mappingKey, datasetCode, instanceKey, general, byDataset);
        return rules == null ? null : aggregateFieldStatus(rules);
    }

    private Map<String, String> findRulesByMapping(JsonNode node, String mappingKey, String datasetCode, String instanceKey, Map<String, Map<String, String>> general, Map<String, Map<String, Map<String, String>>> byDataset) {
        if (!hasMappingKey(node, mappingKey)) return null;

        String logical = text(node, mappingKey, null);
        String localFieldName = localFieldName(mappingKey);
        Map<String, String> rules = null;

        if (instanceKey != null && byDataset != null && !byDataset.isEmpty()) {
            Map<String, Map<String, String>> instanceDetail = byDataset.get(instanceKey);
            rules = findRulesInDetail(instanceDetail, logical, localFieldName, mappingKey);
        }

        if (rules == null && datasetCode != null && byDataset != null && !byDataset.isEmpty()) {
            Map<String, Map<String, String>> datasetDetail = byDataset.get(datasetCode);
            rules = findRulesInDetail(datasetDetail, logical, localFieldName, mappingKey);
        }

        if (rules == null && general != null && !general.isEmpty()) {
            rules = findRulesInDetail(general, logical, localFieldName, mappingKey);
        }

        return rules;
    }

    private String aggregateFieldStatus(Map<String, String> rules) {
        if (rules == null || rules.isEmpty()) return "ERROR";

        boolean hasWarning = false;

        for (String value : rules.values()) {
            if (value == null || value.isBlank()) continue;
            if ("ERROR".equalsIgnoreCase(value)) return "ERROR";
            if ("WARNING".equalsIgnoreCase(value)) hasWarning = true;
        }

        return hasWarning ? "WARNING" : "SUCCESS";
    }

    private String arrayInstanceKey(String datasetCode, String blockName, int index) {
        return (datasetCode == null ? "" : datasetCode) + "#" + blockName + "#" + index;
    }

    private void copyTechnicalKey(JsonNode source, ObjectNode target) {
        if (source == null || target == null) return;

        JsonNode keyNode = source.get(KEY);
        if (keyNode != null && !keyNode.isNull()) target.set(KEY, keyNode.deepCopy());
    }

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        if (src == null || dst == null || fields == null) return;

        for (String field : fields) {
            if (field == null) continue;
            JsonNode value = src.get(field);
            if (value != null && !value.isNull()) dst.set(field, value);
        }
    }

    private static String text(JsonNode node, String field, String def) {
        if (node == null || field == null) return def;
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? def : value.asText(def);
    }

    private Map<String, String> findRulesInDetail(Map<String, Map<String, String>> detail, String logical, String localFieldName, String mappingKey) {
        if (detail == null || detail.isEmpty()) return null;

        Map<String, String> rules = findRulesByKey(detail, logical);
        if (rules != null) return rules;

        rules = findRulesByKey(detail, localFieldName);
        if (rules != null) return rules;

        return findRulesByKey(detail, mappingKey);
    }

    private Map<String, String> findRulesByKey(Map<String, Map<String, String>> detail, String key) {
        if (key == null || key.isBlank()) return null;

        Map<String, String> rules = detail.get(key);
        if (rules != null) return rules;

        return detail.get(key.replace('.', ','));
    }

    private boolean hasMappingKey(JsonNode node, String mappingKey) {
        return node != null && mappingKey != null && node.has(mappingKey);
    }

    private String localFieldName(String mappingKey) {
        if (mappingKey == null || !mappingKey.startsWith(MAPPING_PREFIX)) return mappingKey;
        return mappingKey.substring(MAPPING_PREFIX.length()).trim();
    }
}