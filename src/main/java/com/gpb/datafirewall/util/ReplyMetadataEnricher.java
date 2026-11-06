package com.gpb.datafirewall.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.transaction.TransactionalReply;

import java.time.Instant;
import java.util.Objects;

/**
 * Adds DFW response metadata at the last possible stage before sending a broker reply.
 * This class is the single source of truth for QUERY, ANSWER and ANSWER_DETAIL metadata.
 * The supplied instant is shared by all three representations of one request.
 */
public final class ReplyMetadataEnricher {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ReplyMetadataEnricher() {
    }

    public static void enrich(
            TransactionalReply transaction,
            MessageRecord original,
            Instant completedAt) throws JsonProcessingException {

        Objects.requireNonNull(transaction, "transaction");
        Objects.requireNonNull(original, "original");
        Objects.requireNonNull(completedAt, "completedAt");

        String hostname = HostnameUtils.getHostname();
        String created = DfwDateTimeFormatter.formatEpochMillis(original.getCreatedDttm());
        String readed = DfwDateTimeFormatter.formatEpochMillis(original.getReadedDttm());
        String action = DfwDateTimeFormatter.formatEpochMillis(completedAt.toEpochMilli());
        
        ProcessingResult reply = transaction.reply();
        ProcessingResult audit = transaction.audit();

        enrichAnswer(reply, hostname, created, readed, action);
        
        if (audit != reply) {
            enrichAnswer(audit, hostname, created, readed, action);
        }

        enrichQuery(audit, original, hostname, created, readed, action);
    }

    private static void enrichAnswer(
            ProcessingResult result,
            String hostname,
            String created,
            String readed,
            String action) throws JsonProcessingException {

        ObjectNode answerRoot = readObject(result.getShortJson(), "ANSWER");
        JsonNode data = answerRoot.get("data");

        // Internal ANSWER uses {"data": {...}}; keep the external handler's
        // standalone answer object compatible if it has no data envelope.
        ObjectNode answer = data != null && data.isObject()
                ? (ObjectNode) data : answerRoot;

        putMetadata(
                answer, 
                hostname, 
                created, 
                readed, 
                action
        );

        result.setShortJson(MAPPER.writeValueAsString(answerRoot));

        if (result.getDetailJson() != null && !result.getDetailJson().isBlank()) {
            ObjectNode detail = readObject(result.getDetailJson(), "ANSWER_DETAIL");
            putMetadata(detail, hostname, created, readed, action);
            result.setDetailJson(MAPPER.writeValueAsString(detail));
        }
    }

    private static void enrichQuery(
            ProcessingResult audit,
            MessageRecord original,
            String hostname,
            String created,
            String readed,
            String action) throws JsonProcessingException {

        ObjectNode query = readObject(audit.getOriginalJson(), "QUERY");

        // Одинаковые служебные атрибуты для IBM MQ и Artemis
        putMetadata(query, hostname, created, readed, action);

        if (original.headers != null) {
            query.set("headers", MAPPER.valueToTree(original.headers));
        }

        // Код набора данных для обоих брокеров
        JsonNode answer = MAPPER.readTree(audit.getShortJson());
        JsonNode datasetCode = answer.path("data").get("dfw_dataset_code");

        if (datasetCode != null && !datasetCode.isNull()) {
            query.set("dfw_dataset_code", datasetCode.deepCopy());
        }

        audit.setOriginalJson(MAPPER.writeValueAsString(query));
    }

    private static ObjectNode readObject(String value, String type)
            throws JsonProcessingException {
        JsonNode parsed = MAPPER.readTree(value);
        if (!(parsed instanceof ObjectNode object)) {
            throw new IllegalArgumentException(type + " must be a JSON object");
        }
        return object;
    }

    private static void putMetadata(
            ObjectNode node,
            String hostname,
            String created,
            String readed,
            String action) {
        node.put("dfw_hostname", hostname);
        node.put("dfw_created_dttm", created);
        node.put("dfw_readed_dttm", readed);
        node.put("dfw_action_dttm", action);
    }
}
