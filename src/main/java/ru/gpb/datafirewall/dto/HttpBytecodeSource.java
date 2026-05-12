package ru.gpb.datafirewall.dto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.gpb.datafirewall.config.IgniteRulesApiClient;
import ru.gpb.datafirewall.ignite.BytecodeSource;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class HttpBytecodeSource implements BytecodeSource {
    private static final Logger log = LoggerFactory.getLogger(HttpBytecodeSource.class);

    private final IgniteRulesApiClient apiClient;

    public HttpBytecodeSource(IgniteRulesApiClient apiClient) {
        this.apiClient = Objects.requireNonNull(apiClient, "apiClient");
    }

    /**
     * Загружает compiled rules из versioned cache по полному имени кэша.
     *
     * Пример fullCacheName:
     * - compiled_rules_6
     */
    @Override
    public Map<String, byte[]> loadAll(String fullCacheName) {
        try {
            CacheResponseDto<String, Object> resp = apiClient.getVersionedCache(fullCacheName);
            Map<String, Object> cache = (resp == null) ? null : resp.getCache();

            if (cache == null || cache.isEmpty()) {
                log.info("[RULES] http response fullCacheName={} count=0", fullCacheName);
                return Map.of();
            }

            Base64.Decoder dec = Base64.getDecoder();
            Map<String, byte[]> out = new LinkedHashMap<>();

            cache.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> {
                        String key = e.getKey();
                        Object rawValue = e.getValue();

                        if (key == null || key.isBlank() || rawValue == null) {
                            return;
                        }

                        try {
                            if (rawValue instanceof String b64) {
                                if (!b64.isBlank()) {
                                    out.put(key, dec.decode(b64));
                                }
                            } else {
                                throw new IllegalArgumentException(
                                        "Unexpected value type for key='" + key + "': " +
                                                rawValue.getClass().getName()
                                );
                            }
                        } catch (IllegalArgumentException ex) {
                            throw new IllegalArgumentException(
                                    "Invalid cache payload for key='" + key + "' in fullCacheName='" + fullCacheName + "'",
                                    ex
                            );
                        }
                    });

            log.info("[RULES] http response fullCacheName={} count={}", fullCacheName, out.size());
            return out;

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load compiled rules via HTTP for fullCacheName=" + fullCacheName,
                    e
            );
        }
    }
}