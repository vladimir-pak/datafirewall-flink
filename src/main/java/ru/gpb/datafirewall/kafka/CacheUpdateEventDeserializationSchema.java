package ru.gpb.datafirewall.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.nio.charset.StandardCharsets;

public class CacheUpdateEventDeserializationSchema
        extends AbstractDeserializationSchema<CacheUpdateEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public CacheUpdateEvent deserialize(byte[] message) {
        try {
            String s = new String(message, StandardCharsets.UTF_8);
            JsonNode n = MAPPER.readTree(s);

            long version = n.path("version").asLong(-1);
            String cacheName = n.path("cacheName").asText(null);

            if (cacheName != null) {
                cacheName = cacheName.trim();
            }

            return new CacheUpdateEvent(version, cacheName);
        } catch (Exception e) {
            return new CacheUpdateEvent(-1, null);
        }
    }
}