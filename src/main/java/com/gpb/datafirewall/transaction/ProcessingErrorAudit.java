
package com.gpb.datafirewall.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.util.ReplyMetadataEnricher;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;

public final class ProcessingErrorAudit {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ProcessingErrorAudit() {}

    public static ProcessingResult create(MessageRecord record, Exception failure) throws Exception {
        ObjectNode query = readQuery(record.payload);
        Throwable cause = rootCause(failure);
        ObjectNode error = query.putObject("error");
        error.put("type", cause.getClass().getSimpleName());
        error.put("message", cause.getMessage());
        error.put("component", cause.getStackTrace().length == 0 ? "Unknown" : cause.getStackTrace()[0].getClassName());
        StringWriter writer = new StringWriter();
        failure.printStackTrace(new PrintWriter(writer));
        String stack = writer.toString();
        error.put("stackTrace", stack.length() > 8192 ? stack.substring(0, 8192) : stack);
        ReplyMetadataEnricher.enrichErrorQuery(query, record, Instant.now());
        String json = MAPPER.writeValueAsString(query);
        ProcessingResult result = record.isMq()
                ? ProcessingResult.forMq(record.mqMessageId, null, null, json)
                : ProcessingResult.forJms(record.jmsMessageId, null, null, json);
        result.setHeaders(record.headers);
        result.setCreatedDttm(record.getCreatedDttm());
        result.setReadedDttm(record.getReadedDttm());
        return result;
    }

    private static ObjectNode readQuery(String payload) {
        if (payload == null) {
            ObjectNode fallback = MAPPER.createObjectNode();
            fallback.putNull("rawPayload");
            return fallback;
        }
        try {
            JsonNode node = MAPPER.readTree(payload);
            if (node instanceof ObjectNode object) return object;
            ObjectNode fallback = MAPPER.createObjectNode();
            fallback.set("data", node);
            return fallback;
        } catch (JsonProcessingException error) {
            ObjectNode fallback = MAPPER.createObjectNode();
            fallback.put("rawPayload", payload);
            return fallback;
        }
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) current = current.getCause();
        return current;
    }
}
