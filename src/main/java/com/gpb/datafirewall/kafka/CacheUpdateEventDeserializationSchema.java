package com.gpb.datafirewall.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import java.io.IOException;

public final class CacheUpdateEventDeserializationSchema
        extends AbstractDeserializationSchema<CacheUpdateEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override public CacheUpdateEvent deserialize(byte[] message) throws IOException {
        JsonNode node = MAPPER.readTree(message);
        if (node == null || !node.isObject() || !node.path("version").canConvertToInt()
                || !node.path("version").isIntegralNumber()) {
            throw new IOException("Invalid cache update: version must be an integer");
        }
        CacheUpdateEvent event = new CacheUpdateEvent(node.path("version").intValue(),
                node.path("cacheName").asText(null), node.path("handler").asText(null));
        if (!event.isValid()) throw new IOException("Unsupported cache update: " + event);
        return event;
    }
}
