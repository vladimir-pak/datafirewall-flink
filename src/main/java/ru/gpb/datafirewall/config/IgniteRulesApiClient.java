package ru.gpb.datafirewall.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import ru.gpb.datafirewall.dto.CacheResponseDto;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public final class IgniteRulesApiClient {

    private static final ObjectMapper OM = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String baseUrl;
    private final HttpClient http;

    public IgniteRulesApiClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;

        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    /**
     * Чтение актуальной версии кэша по имени без версии.
     *
     * Используется для стартовой инициализации через latest API.
     * Пример:
     * - compiled_rules
     * - politics
     */
    public CacheResponseDto<String, Object> getActualCache(String cacheName) {
        String url = String.format("%s/api/v1/cache/%s/latest",
                baseUrl, URLEncoder.encode(cacheName, StandardCharsets.UTF_8));

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .GET()
                .build();

        try {
            HttpResponse<String> resp = http.send(
                    req,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            );

            if (resp.statusCode() / 100 != 2) {
                throw new RuntimeException(
                        "Ignite latest cache API HTTP " + resp.statusCode() + " for " + url +
                                ": " + truncate(resp.body(), 800)
                );
            }

            return OM.readValue(
                    resp.body(),
                    OM.getTypeFactory().constructParametricType(
                            CacheResponseDto.class,
                            String.class,
                            Object.class
                    )
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to call Ignite latest cache API: " + url, e);
        }
    }

    /**
     * Чтение versioned cache по полному имени.
     *
     * Примеры:
     * - compiled_rules_12
     * - politics_dataset2control_area_5
     */
    public CacheResponseDto<String, Object> getVersionedCache(String fullCacheName) {
        String url = baseUrl + "/api/v1/cache/" +
                URLEncoder.encode(fullCacheName, StandardCharsets.UTF_8);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .GET()
                .build();

        try {
            HttpResponse<String> resp = http.send(
                    req,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            );

            if (resp.statusCode() / 100 != 2) {
                throw new RuntimeException(
                        "Ignite cache API HTTP " + resp.statusCode() + " for " + url +
                                ": " + truncate(resp.body(), 800)
                );
            }

            return OM.readValue(
                    resp.body(),
                    OM.getTypeFactory().constructParametricType(
                            CacheResponseDto.class,
                            String.class,
                            Object.class
                    )
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to call Ignite cache API: " + url, e);
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}