
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
        assertEquals("IllegalStateException", query.path("error").path("type").asText());
        assertEquals("Test processing failure", query.path("error").path("message").asText());
        assertTrue(query.path("error").path("stackTrace").asText().contains("IllegalStateException"));
        assertNull(result.getShortJson());
        assertNull(result.getDetailJson());
    }

    @Test
    void invalidJsonStillCreatesErrorAudit() throws Exception {
        MessageRecord record = MessageRecord.fromJms("ID:invalid", "invalid-json", null, 1L);
        ProcessingResult result = ProcessingErrorAudit.create(record, new IllegalArgumentException("Invalid request"));
        JsonNode query = MAPPER.readTree(result.getOriginalJson());

        assertEquals("invalid-json", query.path("rawPayload").asText());
        assertEquals("IllegalArgumentException", query.path("error").path("type").asText());
        assertEquals("Invalid request", query.path("error").path("message").asText());
        assertNull(result.getShortJson());
        assertNull(result.getDetailJson());
    }
}
