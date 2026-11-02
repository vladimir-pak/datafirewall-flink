package com.gpb.datafirewall.kafka;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class CacheUpdateEventDeserializationSchemaTest {
    private final CacheUpdateEventDeserializationSchema schema =
            new CacheUpdateEventDeserializationSchema();

    @Test void acceptsIndependentRulesAndPoliticsUpdates() throws Exception {
        CacheUpdateEvent rules = decode("{\"cacheName\":\"rules\",\"version\":72}");
        assertTrue(rules.isRules());
        assertEquals(72, rules.version);
        assertNull(rules.handler);

        CacheUpdateEvent politics = decode("{\"cacheName\":\"politics\",\"version\":65,\"handler\":\"dotnet\"}");
        assertTrue(politics.isPolitics());
        assertEquals("dotnet", politics.handler);
    }

    @Test void acceptsHandlerOnlyWithVersionZero() throws Exception {
        CacheUpdateEvent update = decode("{\"cacheName\":\"handler\",\"version\":0,\"handler\":\"dotnet\"}");
        assertTrue(update.isHandler());
        assertFalse(update.isRules());
        assertFalse(update.isPolitics());
        assertEquals(0, update.version);
        assertEquals("dotnet", update.handler);
    }

    @Test void rejectsInvalidHandlerOnlyMessages() {
        assertThrows(IOException.class,
                () -> decode("{\"cacheName\":\"handler\",\"version\":1,\"handler\":\"flink\"}"));
        assertThrows(IOException.class,
                () -> decode("{\"cacheName\":\"handler\",\"version\":0}"));
        assertThrows(IOException.class,
                () -> decode("{\"cacheName\":\"handler\",\"version\":0,\"handler\":\"other\"}"));
    }

    @Test void rejectsMissingVersionAndUnknownCacheName() {
        assertThrows(IOException.class, () -> decode("{\"cacheName\":\"rules\"}"));
        assertThrows(IOException.class, () -> decode("{\"cacheName\":\"other\",\"version\":1}"));
        assertThrows(IOException.class, () -> decode("{\"cacheName\":\"politics\",\"version\":1.5}"));
    }

    private CacheUpdateEvent decode(String json) throws IOException {
        return schema.deserialize(json.getBytes(StandardCharsets.UTF_8));
    }
}
