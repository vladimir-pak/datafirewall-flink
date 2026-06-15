package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.dto.ProcessingResult;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public final class DotnetHandlerClient {

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final String url;
    private final String jwt;
    private final Duration requestTimeout;

    private final String trustStorePath;
    private final String trustStorePassword;
    private final String trustStoreType;

    private final boolean verifySsl;

    private static final String FIELD_DFW_REQUEST_LATENCY = "dfw_request_latency";
    private static final String FIELD_DFW_CREATED_DTTM = "dfw_created_dttm";
    private static final String FIELD_DFW_READED_DTTM = "dfw_readed_dttm";
    private static final String FIELD_DFW_PROCESS_DTTM = "dfw_process_dttm";

    public DotnetHandlerClient(
            String url,
            String jwt,
            long timeoutMs,
            String trustStorePath,
            String trustStorePassword,
            String trustStoreType
    ) {
        this(url, jwt, timeoutMs, new ObjectMapper(), trustStorePath, trustStorePassword, trustStoreType, true);
    }

    public DotnetHandlerClient(
            String url,
            String jwt,
            long timeoutMs,
            ObjectMapper mapper,
            String trustStorePath,
            String trustStorePassword,
            String trustStoreType,
            boolean verifySsl
    ) {
        this.url = normalizeUrlOrNull(url);
        this.jwt = normalizeJwt(jwt);
        this.requestTimeout = Duration.ofMillis(timeoutMs <= 0 ? 20_000L : timeoutMs);
        this.mapper = mapper == null ? new ObjectMapper() : mapper;
        this.trustStorePath = normalizeUrlOrNull(trustStorePath);
        this.trustStorePassword = trustStorePassword;
        this.trustStoreType = trustStoreType == null || trustStoreType.isBlank()
                ? "PKCS12"
                : trustStoreType.trim();

        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3));

        this.verifySsl = verifySsl;

        // Логика выбора SSL-контекста
        if (!this.verifySsl) {
            // Отключаем проверку SSL
            builder.sslContext(createInsecureSslContext());
        } else if (this.trustStorePath != null) {
            // Используем кастомный truststore
            builder.sslContext(buildSslContext(
                    this.trustStorePath,
                    this.trustStorePassword,
                    this.trustStoreType
            ));
        }

        this.http = builder.build();
    }

    public ProcessingResult process(MessageRecord in) {
        if (in == null) {
            throw new IllegalArgumentException("MessageRecord is null");
        }
        if (in.payload == null || in.payload.isBlank()) {
            return null;
        }
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("handler.dotnet.url must be provided when runtime handler=dotnet");
        }
        if (jwt == null || jwt.isBlank()) {
            throw new IllegalStateException("dotnetJwt must be provided in Vault when runtime handler=dotnet");
        }

        DotnetHttpResponse httpResponse = call(in.payload);
        DotnetHandlerResponse response = parseResponse(
                httpResponse.body(),
                httpResponse.latencyMs(),
                in.createdDttm,
                in.readedDttm
        );
        if (response.shortJson() == null || response.shortJson().isBlank()) {
            return null;
        }

        if (in.mqMessageId != null) {
            return ProcessingResult.forMq(
                    in.mqMessageId,
                    response.shortJson(),
                    response.detailJson(),
                    in.payload
            );
        }

        if (in.jmsMessageId != null && !in.jmsMessageId.isBlank()) {
            return ProcessingResult.forJms(
                    in.jmsMessageId,
                    response.shortJson(),
                    response.detailJson(),
                    in.payload
            );
        }

        throw new IllegalStateException(
                "MessageRecord has neither mqMessageId nor jmsMessageId. Cannot build dotnet ProcessingResult. " + in
        );
    }

    private DotnetHandlerResponse parseResponse(
            String responseJson,
            long requestLatencyMs,
            Long createdDttm,
            Long readedDttm
    ) {
        if (responseJson == null || responseJson.isBlank()) {
            return new DotnetHandlerResponse(null, null);
        }

        try {
            JsonNode root = mapper.readTree(responseJson);

            JsonNode answer = root.get("answer");
            JsonNode detailAnswer = root.get("detail_answer");

            if (answer == null || answer.isNull()) {
                throw new IllegalArgumentException("Dotnet handler response does not contain required field 'answer'");
            }

            ObjectNode answerObject;
            if (answer.isObject()) {
                answerObject = (ObjectNode) answer.deepCopy();
            } else {
                answerObject = mapper.createObjectNode();
                answerObject.set("value", answer);
            }

            ObjectNode detailAnswerObject;
            if (detailAnswer != null && detailAnswer.isObject()) {
                detailAnswerObject = (ObjectNode) detailAnswer.deepCopy();
            } else if (detailAnswer != null && !detailAnswer.isNull()) {
                detailAnswerObject = mapper.createObjectNode();
                detailAnswerObject.set("value", detailAnswer);
            } else {
                detailAnswerObject = mapper.createObjectNode();
            }

            if (createdDttm != null) {
                detailAnswerObject.put(FIELD_DFW_CREATED_DTTM, createdDttm);
            }

            if (readedDttm != null) {
                detailAnswerObject.put(FIELD_DFW_READED_DTTM, readedDttm);
            }

            detailAnswerObject.put(FIELD_DFW_REQUEST_LATENCY, requestLatencyMs);
            detailAnswerObject.put(FIELD_DFW_PROCESS_DTTM, currentTimestampMs());

            String shortJson = mapper.writeValueAsString(answerObject);
            String detailJson = mapper.writeValueAsString(detailAnswerObject);

            return new DotnetHandlerResponse(shortJson, detailJson);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse dotnet handler response. Expected JSON: {\"answer\": ObjectNode, \"detail_answer\": ObjectNode}. Body="
                            + truncate(responseJson, 800),
                    e
            );
        }
    }

    private DotnetHttpResponse call(String payload) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Authorization", "Bearer " + jwt)
                .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));

        HttpRequest request = builder.build();

        long startedAtNs = System.nanoTime();

        try {
            HttpResponse<String> response = http.send(
                    request,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            );

            long latencyMs = elapsedMs(startedAtNs);

            if (response.statusCode() / 100 != 2) {
                throw new RuntimeException(
                        "Dotnet handler HTTP " + response.statusCode() +
                                " for " + url +
                                ", latencyMs=" + latencyMs +
                                ": " + truncate(response.body(), 800)
                );
            }

            return new DotnetHttpResponse(response.body(), latencyMs);
        } catch (Exception e) {
            long latencyMs = elapsedMs(startedAtNs);
            throw new RuntimeException(
                    "Failed to call dotnet handler API: " + url + ", latencyMs=" + latencyMs,
                    e
            );
        }
    }

    private static String normalizeUrlOrNull(String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        return url.trim();
    }

    private static String normalizeJwt(String jwt) {
        if (jwt == null || jwt.isBlank()) {
            return null;
        }
        String value = jwt.trim();
        if (value.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
            return value.substring("Bearer ".length()).trim();
        }
        return value;
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private static SSLContext buildSslContext(
            String trustStorePath,
            String trustStorePassword,
            String trustStoreType
    ) {
        try {
            KeyStore trustStore = KeyStore.getInstance(
                    trustStoreType == null || trustStoreType.isBlank()
                            ? "PKCS12"
                            : trustStoreType
            );

            try (InputStream in = Files.newInputStream(Path.of(trustStorePath))) {
                trustStore.load(
                        in,
                        trustStorePassword == null ? null : trustStorePassword.toCharArray()
                );
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
                    "Failed to build SSLContext for dotnet handler. trustStore=" +
                            trustStorePath + ", trustStoreType=" + trustStoreType,
                    e
            );
        }
    }

    /**
     * Создает небезопасный SSL-контекст, который принимает все сертификаты.
     */
    private static SSLContext createInsecureSslContext() {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {
                            // Доверяем всем клиентским сертификатам
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] chain, String authType) {
                            // Доверяем всем серверным сертификатам
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create insecure SSL context", e);
        }
    }

    private static long currentTimestampMs() {
        return Instant.now().toEpochMilli();
    }

    private record DotnetHandlerResponse(String shortJson, String detailJson) {
    }

    private static long elapsedMs(long startedAtNs) {
        return (System.nanoTime() - startedAtNs) / 1_000_000L;
    }

    private record DotnetHttpResponse(String body, long latencyMs) {
    }
}
