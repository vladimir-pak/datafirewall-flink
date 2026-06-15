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

    private static final String FIELD_DFW_CREATED_DTTM = "dfw_created_dttm";
    private static final String FIELD_DFW_READED_DTTM = "dfw_readed_dttm";
    private static final String FIELD_DFW_DOTNET_PROCESS_START_DTTM = "dfw_dotnet_process_start_dttm";
    private static final String FIELD_DFW_REQUEST_START_DTTM = "dfw_request_start_dttm";
    private static final String FIELD_DFW_REQUEST_END_DTTM = "dfw_request_end_dttm";
    private static final String FIELD_DFW_REQUEST_LATENCY = "dfw_request_latency";
    private static final String FIELD_DFW_PROCESS_DTTM = "dfw_process_dttm";
    private static final String FIELD_DFW_FLINK_QUEUE_LATENCY = "dfw_flink_queue_latency";

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
        this.verifySsl = verifySsl;

        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3));

        if (!this.verifySsl) {
            builder.sslContext(createInsecureSslContext());
        } else if (this.trustStorePath != null) {
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

        /*
         * Время, когда сообщение реально дошло до DotnetHandlerClient.process(...)
         * Если dfw_readed_dttm сильно раньше этого времени — сообщение ждало внутри Flink pipeline.
         */
        long dotnetProcessStartDttm = currentTimestampMs();

        DotnetHttpResponse httpResponse = call(in.payload);

        DotnetHandlerResponse response = parseResponse(
                httpResponse.body(),
                in.createdDttm,
                in.readedDttm,
                dotnetProcessStartDttm,
                httpResponse.requestStartDttm(),
                httpResponse.requestEndDttm(),
                httpResponse.latencyMs()
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
            Long createdDttm,
            Long readedDttm,
            long dotnetProcessStartDttm,
            long requestStartDttm,
            long requestEndDttm,
            long requestLatencyMs
    ) {
        if (responseJson == null || responseJson.isBlank()) {
            return new DotnetHandlerResponse(null, null);
        }

        try {
            JsonNode root = mapper.readTree(responseJson);

            JsonNode answer = root.get("answer");
            JsonNode detailAnswer = root.get("detail_answer");
            if (detailAnswer == null || detailAnswer.isNull()) {
                detailAnswer = root.get("detailAnswer");
            }

            if (answer == null || answer.isNull()) {
                throw new IllegalArgumentException("Dotnet handler response does not contain required field 'answer'");
            }

            ObjectNode answerObject = toObjectNode(answer);

            ObjectNode detailAnswerObject;
            if (detailAnswer == null || detailAnswer.isNull()) {
                detailAnswerObject = mapper.createObjectNode();
            } else {
                detailAnswerObject = toObjectNode(detailAnswer);
            }

            enrichDetailAnswerWithTimings(
                    detailAnswerObject,
                    createdDttm,
                    readedDttm,
                    dotnetProcessStartDttm,
                    requestStartDttm,
                    requestEndDttm,
                    requestLatencyMs
            );

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

    private void enrichDetailAnswerWithTimings(
            ObjectNode detailAnswerObject,
            Long createdDttm,
            Long readedDttm,
            long dotnetProcessStartDttm,
            long requestStartDttm,
            long requestEndDttm,
            long requestLatencyMs
    ) {
        if (createdDttm != null) {
            detailAnswerObject.put(FIELD_DFW_CREATED_DTTM, createdDttm);
        }

        if (readedDttm != null) {
            detailAnswerObject.put(FIELD_DFW_READED_DTTM, readedDttm);
        }

        detailAnswerObject.put(FIELD_DFW_DOTNET_PROCESS_START_DTTM, dotnetProcessStartDttm);
        detailAnswerObject.put(FIELD_DFW_REQUEST_START_DTTM, requestStartDttm);
        detailAnswerObject.put(FIELD_DFW_REQUEST_END_DTTM, requestEndDttm);
        detailAnswerObject.put(FIELD_DFW_REQUEST_LATENCY, requestLatencyMs);

        /*
         * Максимально близкое к завершению обработки в DotnetHandlerClient.
         * После этого остается только сериализация detailJson/shortJson и создание ProcessingResult.
         */
        long processDttm = currentTimestampMs();
        detailAnswerObject.put(FIELD_DFW_PROCESS_DTTM, processDttm);

        if (readedDttm != null) {
            detailAnswerObject.put(
                    FIELD_DFW_FLINK_QUEUE_LATENCY,
                    dotnetProcessStartDttm - readedDttm
            );
        }
    }

    private ObjectNode toObjectNode(JsonNode node) {
        if (node != null && node.isObject()) {
            return (ObjectNode) node.deepCopy();
        }

        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.set("value", node);
        return objectNode;
    }

    private DotnetHttpResponse call(String payload) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Authorization", "Bearer " + jwt)
                .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
                .build();

        long requestStartDttm = currentTimestampMs();
        long startedAtNs = System.nanoTime();

        try {
            HttpResponse<String> response = http.send(
                    request,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            );

            long requestEndDttm = currentTimestampMs();
            long latencyMs = elapsedMs(startedAtNs);

            if (response.statusCode() / 100 != 2) {
                throw new RuntimeException(
                        "Dotnet handler HTTP " + response.statusCode() +
                                " for " + url +
                                ", latencyMs=" + latencyMs +
                                ": " + truncate(response.body(), 800)
                );
            }

            return new DotnetHttpResponse(
                    response.body(),
                    latencyMs,
                    requestStartDttm,
                    requestEndDttm
            );
        } catch (Exception e) {
            long requestEndDttm = currentTimestampMs();
            long latencyMs = elapsedMs(startedAtNs);

            throw new RuntimeException(
                    "Failed to call dotnet handler API: " + url +
                            ", requestStartDttm=" + requestStartDttm +
                            ", requestEndDttm=" + requestEndDttm +
                            ", latencyMs=" + latencyMs,
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
     * Небезопасный SSLContext: доверяет любому сертификату.
     * Использовать только для dev/test.
     */
    private static SSLContext createInsecureSslContext() {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {
                            // trust all
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] chain, String authType) {
                            // trust all
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

    private static long elapsedMs(long startedAtNs) {
        return (System.nanoTime() - startedAtNs) / 1_000_000L;
    }

    private record DotnetHandlerResponse(
            String shortJson,
            String detailJson
    ) {
    }

    private record DotnetHttpResponse(
            String body,
            long latencyMs,
            long requestStartDttm,
            long requestEndDttm
    ) {
    }
}