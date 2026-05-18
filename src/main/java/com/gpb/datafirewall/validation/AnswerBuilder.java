package com.gpb.datafirewall.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class AnswerBuilder {

    private final ObjectMapper mapper;

    public AnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode buildAnswer(
        JsonNode originalEvent,
        ValidationResult validation,
        String qid,
        Long createdDttm,
        Long readedDttm
    ) {
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

        String now = Instant.now().toString();
        // out.put("dfw_readed_buf_dttm", now);
        // out.put("dfw_sending_to_mq_dttm", now);
        // out.put("dfw_action_dttm", now);

        // if (out.get("dfw_created_dttm") == null) {
        //     out.put("dfw_created_dttm", now);
        // }
        out.put("dfw_created_dttm", createdDttm);
        out.put("dfw_readed_dttm", readedDttm);
        out.put("dfw_action_dttm", now);
        out.put("dfw_query_id", qid);

        String processStatus = (validation == null || validation.processStatus() == null)
                ? "ERROR"
                : validation.processStatus();
        out.put("PROCESS_STATUS", processStatus);

        out.set("details", buildShortDetails(originalEvent, validation));
        out.set("errors", buildErrors(originalEvent, validation));

        return out;
    }

    private ObjectNode buildShortDetails(JsonNode originalEvent, ValidationResult validation) {
        ObjectNode details = mapper.createObjectNode();

        String all = (validation == null || validation.allResult() == null)
                ? "ERROR"
                : validation.allResult();
        details.put("ALL_RESULT", all);

        Map<String, Map<String, String>> general =
                (validation == null || validation.detailByField() == null)
                        ? Map.of()
                        : validation.detailByField();

        Map<String, Map<String, Map<String, String>>> byDataset =
                (validation == null || validation.detailByDataset() == null)
                        ? Map.of()
                        : validation.detailByDataset();

        JsonNode data = (originalEvent == null) ? null : originalEvent.get("data");

        JsonNode homeAddressNode = data == null ? null : firstExisting(data, "homeAddress", "addressRegistration", "registrationAddress");
        JsonNode registrationAddressNode = data == null ? null : firstExisting(data, "registrationAddress", "addressRegistration");

        String homeDatasetCode = text(homeAddressNode, "dataset_code", "УС.ЛиК.Адрес проживания");
        String regDatasetCode = text(registrationAddressNode, "dataset_code", "УС.ЛиК.Адрес регистрации");

        ObjectNode homeAddressShortNode = buildAddressShortNode(
                homeAddressNode,
                homeDatasetCode,
                general,
                byDataset
        );
        if (homeAddressShortNode.size() > 0) {
            details.set("homeAddress", homeAddressShortNode);
        }

        ObjectNode registrationAddressShortNode = buildAddressShortNode(
                registrationAddressNode,
                regDatasetCode,
                general,
                byDataset
        );
        if (registrationAddressShortNode.size() > 0) {
            details.set("registrationAddress", registrationAddressShortNode);
        }

        appendDynamicShortDetails(
                details,
                data,
                general,
                byDataset,
                Set.of("homeAddress", "registrationAddress", "addressRegistration")
        );

        return details;
    }

    private ObjectNode buildErrors(JsonNode originalEvent, ValidationResult validation) {
        ObjectNode errors = mapper.createObjectNode();

        Map<String, List<String>> errorsByField =
                (validation == null || validation.errorsByField() == null)
                        ? Map.of()
                        : validation.errorsByField();

        if (errorsByField.isEmpty()) {
            return errors;
        }

        JsonNode data = (originalEvent == null) ? null : originalEvent.get("data");
        if (data == null || !data.isObject()) {
            return errors;
        }

        Iterator<Map.Entry<String, JsonNode>> it = data.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> blockEntry = it.next();
            String blockName = blockEntry.getKey();
            JsonNode blockNode = blockEntry.getValue();

            if (blockName == null || blockNode == null || !blockNode.isObject()) {
                continue;
            }

            ObjectNode blockErrors = buildBlockErrors(blockNode, errorsByField);

            if ("documents".equals(blockName)) {
                ArrayNode cardsErrors = buildClientIdCardErrors(blockNode, errorsByField);
                if (cardsErrors.size() > 0) {
                    blockErrors.set("clientIdCard", cardsErrors);
                }
            }

            if (blockErrors.size() > 0) {
                errors.set(blockName, blockErrors);
            }
        }

        return errors;
    }

    private ObjectNode buildBlockErrors(JsonNode blockNode, Map<String, List<String>> errorsByField) {
        ObjectNode blockErrors = mapper.createObjectNode();

        Iterator<Map.Entry<String, JsonNode>> fields = blockNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();
            String key = e.getKey();
            JsonNode value = e.getValue();

            if (key == null || !key.startsWith("mapping.") || value == null || value.isNull()) {
                continue;
            }

            String localFieldName = key.substring("mapping.".length()).trim();
            String logicalField = value.asText(null);

            if (logicalField == null || logicalField.isBlank() || "none".equalsIgnoreCase(logicalField)) {
                continue;
            }

            List<String> fieldErrors = findErrorsByLogicalField(errorsByField, logicalField);
            if (fieldErrors != null && !fieldErrors.isEmpty()) {
                ArrayNode arr = mapper.createArrayNode();
                for (String msg : fieldErrors) {
                    if (msg != null) {
                        arr.add(msg);
                    }
                }
                if (arr.size() > 0) {
                    blockErrors.set(localFieldName, arr);
                }
            }
        }

        return blockErrors;
    }

    private ArrayNode buildClientIdCardErrors(JsonNode documentsNode, Map<String, List<String>> errorsByField) {
        ArrayNode result = mapper.createArrayNode();

        JsonNode cards = documentsNode == null ? null : documentsNode.get("clientIdCard");
        if (cards == null || !cards.isArray() || cards.isEmpty()) {
            return result;
        }

        for (JsonNode cardNode : cards) {
            if (cardNode == null || !cardNode.isObject()) {
                continue;
            }

            ObjectNode cardErrors = buildBlockErrors(cardNode, errorsByField);
            if (cardErrors.size() > 0) {
                result.add(cardErrors);
            }
        }

        return result;
    }

    private void appendDynamicShortDetails(
            ObjectNode details,
            JsonNode data,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset,
            Set<String> excludedBlocks
    ) {
        if (details == null || data == null || !data.isObject()) {
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> blocks = data.fields();
        while (blocks.hasNext()) {
            Map.Entry<String, JsonNode> blockEntry = blocks.next();
            String blockName = blockEntry.getKey();
            JsonNode blockNode = blockEntry.getValue();

            if (blockName == null || blockNode == null || !blockNode.isObject()) {
                continue;
            }
            if (excludedBlocks != null && excludedBlocks.contains(blockName)) {
                continue;
            }

            ObjectNode blockShortNode = buildGenericShortNode(blockNode, general, byDataset);
            if (blockShortNode.size() > 0) {
                details.set(blockName, blockShortNode);
            }
        }
    }

    private ObjectNode buildGenericShortNode(
            JsonNode sourceNode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        ObjectNode result = mapper.createObjectNode();
        if (sourceNode == null || !sourceNode.isObject()) {
            return result;
        }

        String datasetCode = text(sourceNode, "dataset_code", null);

        appendMappedFieldStatuses(result, sourceNode, datasetCode, general, byDataset);
        appendNestedShortNodes(result, sourceNode, general, byDataset);

        if (result.size() == 0) {
            return result;
        }

        if (datasetCode == null) {
            return result;
        }

        ObjectNode withDatasetCode = mapper.createObjectNode();
        withDatasetCode.put("dataset_code", datasetCode);
        withDatasetCode.setAll(result);
        return withDatasetCode;
    }

    private void appendMappedFieldStatuses(
            ObjectNode result,
            JsonNode sourceNode,
            String datasetCode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        Iterator<Map.Entry<String, JsonNode>> fields = sourceNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> fieldEntry = fields.next();
            String key = fieldEntry.getKey();

            if (key == null || !key.startsWith("mapping.")) {
                continue;
            }

            String localFieldName = key.substring("mapping.".length()).trim();
            if (localFieldName.isEmpty()) {
                continue;
            }

            String status = statusByMappingFlexibleOrNull(sourceNode, key, datasetCode, general, byDataset);
            if (status != null) {
                result.put(localFieldName, status);
            }
        }
    }

    private void appendNestedShortNodes(
            ObjectNode result,
            JsonNode sourceNode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        Iterator<Map.Entry<String, JsonNode>> fields = sourceNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> fieldEntry = fields.next();
            String key = fieldEntry.getKey();
            JsonNode value = fieldEntry.getValue();

            if (key == null || key.startsWith("mapping.") || "dataset_code".equals(key)) {
                continue;
            }
            if (value == null || value.isNull()) {
                continue;
            }

            if (value.isObject()) {
                ObjectNode child = buildGenericShortNode(value, general, byDataset);
                if (child.size() > 0) {
                    result.set(key, child);
                }
                continue;
            }

            if (value.isArray()) {
                ArrayNode children = buildGenericShortArray(value, general, byDataset);
                if (children.size() > 0) {
                    result.set(key, children);
                }
            }
        }
    }

    private ArrayNode buildGenericShortArray(
            JsonNode arrayNode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        ArrayNode result = mapper.createArrayNode();
        if (arrayNode == null || !arrayNode.isArray()) {
            return result;
        }

        for (JsonNode item : arrayNode) {
            if (item == null || !item.isObject()) {
                continue;
            }

            ObjectNode child = buildGenericShortNode(item, general, byDataset);
            if (child.size() == 0) {
                continue;
            }

            String elemId = text(item, "elemId", null);
            if (elemId != null && !child.has("elemId")) {
                child.put("elemId", elemId);
            }

            result.add(child);
        }

        return result;
    }

    private List<String> findErrorsByLogicalField(Map<String, List<String>> errorsByField, String logicalField) {
        if (logicalField == null || logicalField.isBlank() || errorsByField == null || errorsByField.isEmpty()) {
            return null;
        }

        List<String> errors = errorsByField.get(logicalField);
        if (errors != null && !errors.isEmpty()) {
            return errors;
        }

        String alt = logicalField.replace('.', ',');
        errors = errorsByField.get(alt);
        if (errors != null && !errors.isEmpty()) {
            return errors;
        }

        return null;
    }

    private ObjectNode buildAddressShortNode(
            JsonNode addr,
            String datasetCode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        ObjectNode fields = mapper.createObjectNode();
        putStatusIfPresent(fields, "city", addr, "mapping.city", datasetCode, general, byDataset);
        putStatusIfPresent(fields, "countryCode", addr, "mapping.countryCode", datasetCode, general, byDataset);
        putStatusIfPresent(fields, "postalCode", addr, "mapping.postalCode", datasetCode, general, byDataset);
        putStatusIfPresent(fields, "street", addr, "mapping.street", datasetCode, general, byDataset);
        putStatusIfPresent(fields, "area", addr, "mapping.area", datasetCode, general, byDataset);
        putStatusIfPresent(fields, "countryName", addr, "mapping.countryName", datasetCode, general, byDataset);
        putStatusIfPresent(fields, "settlement", addr, "mapping.settlement", datasetCode, general, byDataset);

        if (fields.size() == 0) {
            return fields;
        }

        ObjectNode o = mapper.createObjectNode();
        o.put("dataset_code", datasetCode);
        o.setAll(fields);
        return o;
    }

    // private ObjectNode buildContactShortNode(JsonNode contact, Map<String, Map<String, String>> detailByField) {
    //     ObjectNode o = mapper.createObjectNode();
    //     o.put("dataset_code", text(contact, "dataset_code", "УС.ЛИК.Контакты клиента"));

    //     o.put("mobilePhone", statusByMappingFlexibleOld(contact, "mapping.mobilePhone", detailByField));
    //     o.put("emailValue", statusByMappingFlexibleOld(contact, "mapping.emailValue", detailByField));

    //     return o;
    // }

    // private ObjectNode buildBaseInfoShortNode(JsonNode base, Map<String, Map<String, String>> detailByField) {
    //     ObjectNode o = mapper.createObjectNode();
    //     o.put("dataset_code", text(base, "dataset_code", "УС.ЛиК.Данные клиента"));

    //     o.put("citizenship", statusByMappingFlexibleOld(base, "mapping.citizenship", detailByField));
    //     o.put("birthPlace", statusByMappingFlexibleOld(base, "mapping.birthPlace", detailByField));
    //     o.put("surname", statusByMappingFlexibleOld(base, "mapping.surname", detailByField));
    //     o.put("name", statusByMappingFlexibleOld(base, "mapping.name", detailByField));
    //     o.put("gender", statusByMappingFlexibleOld(base, "mapping.gender", detailByField));
    //     o.put("fullName", statusByMappingFlexibleOld(base, "mapping.fullName", detailByField));
    //     o.put("birthdate", statusByMappingFlexibleOld(base, "mapping.birthdate", detailByField));
    //     o.put("patronymic", statusByMappingFlexibleOld(base, "mapping.patronymic", detailByField));

    //     return o;
    // }

    // private ObjectNode buildDocumentsShortNode(JsonNode docs, Map<String, Map<String, String>> detailByField) {
    //     ObjectNode o = mapper.createObjectNode();
    //     o.put("dataset_code", text(docs, "dataset_code", "УС.ЛиК.Документы клиента"));

    //     o.put("clientInn", statusByMappingFlexibleOld(docs, "mapping.clientInn", detailByField));
    //     o.put("clientSnils", statusByMappingFlexibleOld(docs, "mapping.clientSnils", detailByField));

    //     ArrayNode arr = mapper.createArrayNode();
    //     JsonNode cards = docs == null ? null : docs.get("clientIdCard");
    //     if (cards != null && cards.isArray() && cards.size() > 0) {
    //         JsonNode card0 = cards.get(0);
    //         ObjectNode c = mapper.createObjectNode();

    //         c.put("elemId", text(card0, "elemId", text(card0, "type", "UNKNOWN")));

    //         c.put("issueAuthority", statusByMappingFlexibleOld(card0, "mapping.issueAuthority", detailByField));
    //         c.put("number", statusByMappingFlexibleOld(card0, "mapping.number", detailByField));
    //         c.put("issueDate", statusByMappingFlexibleOld(card0, "mapping.issueDate", detailByField));
    //         c.put("departmentCode", statusByMappingFlexibleOld(card0, "mapping.departmentCode", detailByField));
    //         c.put("series", statusByMappingFlexibleOld(card0, "mapping.series", detailByField));

    //         arr.add(c);
    //     }
    //     o.set("clientIdCard", arr);

    //     return o;
    // }

    private void putStatusIfPresent(
            ObjectNode target,
            String responseFieldName,
            JsonNode node,
            String mappingKey,
            String datasetCode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        String status = statusByMappingFlexibleOrNull(node, mappingKey, datasetCode, general, byDataset);
        if (status != null) {
            target.put(responseFieldName, status);
        }
    }

    private String statusByMappingFlexibleOrNull(
            JsonNode node,
            String mappingKey,
            String datasetCode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        Map<String, String> rules = findRulesByMapping(node, mappingKey, datasetCode, general, byDataset);
        return rules == null ? null : aggregateFieldStatus(rules);
    }

    private String statusByMappingFlexible(
            JsonNode node,
            String mappingKey,
            String datasetCode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        Map<String, String> rules = findRulesByMapping(node, mappingKey, datasetCode, general, byDataset);
        return aggregateFieldStatus(rules);
    }

    private Map<String, String> findRulesByMapping(
            JsonNode node,
            String mappingKey,
            String datasetCode,
            Map<String, Map<String, String>> general,
            Map<String, Map<String, Map<String, String>>> byDataset
    ) {
        String logical = text(node, mappingKey, null);
        if (logical == null || logical.isBlank() || "none".equalsIgnoreCase(logical)) {
            return null;
        }

        Map<String, String> rules = null;

        if (datasetCode != null && byDataset != null && !byDataset.isEmpty()) {
            Map<String, Map<String, String>> datasetDetail = byDataset.get(datasetCode);
            if (datasetDetail != null) {
                rules = datasetDetail.get(logical);
                if (rules == null) {
                    rules = datasetDetail.get(logical.replace('.', ','));
                }
            }
        }

        if (rules == null && general != null && !general.isEmpty()) {
            rules = general.get(logical);
            if (rules == null) {
                rules = general.get(logical.replace('.', ','));
            }
        }

        return rules;
    }

    // private String statusByMappingFlexibleOld(
    //         JsonNode node,
    //         String mappingKey,
    //         Map<String, Map<String, String>> detailByField
    // ) {
    //     String logical = text(node, mappingKey, null);
    //     if (logical == null || logical.isBlank() || "none".equalsIgnoreCase(logical)) {
    //         return "ERROR";
    //     }

    //     Map<String, String> rules = detailByField.get(logical);
    //     if (rules == null) {
    //         rules = detailByField.get(logical.replace('.', ','));
    //     }

    //     return aggregateFieldStatus(rules);
    // }

    private String aggregateFieldStatus(Map<String, String> rules) {
        if (rules == null || rules.isEmpty()) {
            return "ERROR";
        }
        for (String v : rules.values()) {
            if (v != null && "ERROR".equalsIgnoreCase(v)) {
                return "ERROR";
            }
        }
        return "SUCCESS";
    }

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        if (src == null || dst == null || fields == null) return;
        for (String f : fields) {
            if (f == null) continue;
            JsonNode v = src.get(f);
            if (v != null && !v.isNull()) dst.set(f, v);
        }
    }

    private static String text(JsonNode node, String field, String def) {
        if (node == null || field == null) return def;
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? def : v.asText(def);
    }

    private static JsonNode firstExisting(JsonNode node, String... names) {
        if (node == null || names == null) return null;
        for (String name : names) {
            if (name == null) continue;
            JsonNode found = node.get(name);
            if (found != null && !found.isNull()) {
                return found;
            }
        }
        return null;
    }
}