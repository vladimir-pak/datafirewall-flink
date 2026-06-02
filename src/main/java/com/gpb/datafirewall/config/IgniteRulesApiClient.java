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
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.Collection;

public final class IgniteRulesApiClient {

    private static final ObjectMapper OM = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String baseUrl;
    private final HttpClient http;
    private final String jwt;

    /**
     * Работает для:
     * - http:// без TLS
     * - https:// с дефолтным JVM truststore
     */
    public IgniteRulesApiClient(String baseUrl) {
        this(baseUrl, null, null);
    }

    /**
     * @param baseUrl например https://ignite-api-host:9200
     * @param caPemPath путь до PEM-файла с CA/server chain, например /opt/flink/certs/ca.pem
     * @param jwt jwt для авторизации в datafirewall-spring
     */
    public IgniteRulesApiClient(String baseUrl, String caPemPath) {
        this(baseUrl, caPemPath, null);
    }

    public IgniteRulesApiClient(String baseUrl, String caPemPath, String jwt) {
        this.baseUrl = normalizeBaseUrl(baseUrl);
        this.jwt = jwt;

        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3));

        if (isNotBlank(caPemPath)) {
            builder.sslContext(buildSslContextFromPem(caPemPath));
        } else if (this.baseUrl.startsWith("https://")) {
            System.out.println("WARN: Ignite API uses HTTPS but PEM trust config is not set. JVM default truststore will be used.");
        }

        this.http = builder.build();
    }

    public CacheResponseDto<String, Object> getActualCache(String cacheName) {
        String url = String.format("%s/api/v1/cache/%s/latest",
                baseUrl, URLEncoder.encode(cacheName, StandardCharsets.UTF_8));

        HttpRequest req = requestBuilder(url).GET().build();

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

        HttpRequest req = requestBuilder(url).GET().build();

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

    private static SSLContext buildSslContextFromPem(String caPemPath) {
        try {
            KeyStore trustStore = loadPemTrustStore(caPemPath);

            System.out.println("Ignite API PEM truststore loaded. path=" + caPemPath
                    + ", size=" + trustStore.size());

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm()
            );
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);

            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to build SSLContext from PEM for Ignite API. caPemPath=" + caPemPath,
                    e
            );
        }
    }

    private static KeyStore loadPemTrustStore(String pemPath) throws Exception {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        try (InputStream in = new FileInputStream(pemPath)) {
            Collection<? extends Certificate> certificates =
                    certificateFactory.generateCertificates(in);

            int i = 0;
            for (Certificate certificate : certificates) {
                trustStore.setCertificateEntry("pem-cert-" + i, certificate);
                i++;
            }

            if (i == 0) {
                throw new IllegalArgumentException("No certificates found in PEM file: " + pemPath);
            }
        }

        return trustStore;
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

    private HttpRequest.Builder requestBuilder(String url) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20));

        if (isNotBlank(jwt)) {
            builder.header("Authorization", "Bearer " + jwt);
        }

        return builder;
    }
}