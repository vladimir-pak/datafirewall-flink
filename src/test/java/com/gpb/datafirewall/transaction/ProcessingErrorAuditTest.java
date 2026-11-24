
package com.gpb.datafirewall.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProcessingErrorAuditTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void processingFailureCreatesErrorAudit() throws Exception {
        MessageRecord record = MessageRecord.fromJms("ID:test", "{\"data\":{\"clientId\":\"123\"}}", null, 1L);
        ProcessingResult result = ProcessingErrorAudit.create(record, new IllegalStateException("Test processing failure"));
        JsonNode query = MAPPER.readTree(result.getOriginalJson());

        assertEquals("ID:test", result.getJmsCorrelationId());
        assertEquals("123", query.path("data").path("clientId").asText());
        assertEquals("IllegalStateException", MAPPER.readTree(result.getErrorJson()).path("type").asText());
        assertEquals("Test processing failure", MAPPER.readTree(result.getErrorJson()).path("message").asText());
        assertTrue(MAPPER.readTree(result.getErrorJson()).path("stackTrace").asText().contains("IllegalStateException"));
        assertFalse(query.has("error"));
        assertNull(result.getShortJson());
        assertNull(result.getDetailJson());
    }

    @Test
    void invalidJsonStillCreatesErrorAudit() throws Exception {
        MessageRecord record = MessageRecord.fromJms("ID:invalid", "invalid-json", null, 1L);
        ProcessingResult result = ProcessingErrorAudit.create(record, new IllegalArgumentException("Invalid request"));
        JsonNode query = MAPPER.readTree(result.getOriginalJson());

        assertEquals("invalid-json", query.path("rawPayload").asText());
        assertEquals("IllegalArgumentException", MAPPER.readTree(result.getErrorJson()).path("type").asText());
        assertEquals("Invalid request", MAPPER.readTree(result.getErrorJson()).path("message").asText());
        assertFalse(query.has("error"));
        assertNull(result.getShortJson());
        assertNull(result.getDetailJson());
    }
}
