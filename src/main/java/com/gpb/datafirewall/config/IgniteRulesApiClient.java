package com.gpb.datafirewall.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.dto.CacheResponseDto;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.time.Duration;

public final class IgniteRulesApiClient {

    private static final ObjectMapper OM = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String baseUrl;
    private final HttpClient http;

    /**
     * Старый конструктор.
     * Работает для http:// или https:// с дефолтным JVM truststore.
     */
    public IgniteRulesApiClient(String baseUrl) {
        this(baseUrl, null, null, null);
    }

    /**
     * Новый конструктор с truststore.
     *
     * @param baseUrl              например https://ignite-api-host:8443
     * @param trustStorePath       путь до truststore, например /opt/flink/certs/truststore.jks
     * @param trustStorePassword   пароль truststore
     * @param trustStoreType       JKS или PKCS12. Если null/blank, будет JKS.
     */
    public IgniteRulesApiClient(
            String baseUrl,
            String trustStorePath,
            String trustStorePassword,
            String trustStoreType
    ) {
        this.baseUrl = normalizeBaseUrl(baseUrl);

        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3));

        if (isNotBlank(trustStorePath)) {
            builder.sslContext(buildSslContext(
                    trustStorePath,
                    trustStorePassword,
                    isNotBlank(trustStoreType) ? trustStoreType : "JKS"
            ));
        }

        this.http = builder.build();
    }

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

    private static SSLContext buildSslContext(
            String trustStorePath,
            String trustStorePassword,
            String trustStoreType
    ) {
        try {
            KeyStore trustStore = KeyStore.getInstance(trustStoreType);

            char[] password = trustStorePassword == null
                    ? new char[0]
                    : trustStorePassword.toCharArray();

            try (InputStream in = new FileInputStream(trustStorePath)) {
                trustStore.load(in, password);
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm()
            );
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);

            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to build SSLContext for Ignite API. trustStorePath=" + trustStorePath +
                            ", trustStoreType=" + trustStoreType,
                    e
            );
        }
    }

    private static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("ignite.apiUrl must not be blank");
        }

        return baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}