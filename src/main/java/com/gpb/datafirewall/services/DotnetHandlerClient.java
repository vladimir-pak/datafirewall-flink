package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.dto.ProcessingResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log =
            LoggerFactory.getLogger(DotnetHandlerClient.class);

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
        this(
                url,
                jwt,
                timeoutMs,
                new ObjectMapper(),
                trustStorePath,
                trustStorePassword,
                trustStoreType,
                true
        );
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
        this.requestTimeout =
                Duration.ofMillis(timeoutMs <= 0 ? 20_000L : timeoutMs);

        this.mapper =
                mapper == null ? new ObjectMapper() : mapper;

        this.trustStorePath =
                normalizeUrlOrNull(trustStorePath);

        this.trustStorePassword = trustStorePassword;

        this.trustStoreType =
                trustStoreType == null || trustStoreType.isBlank()
                        ? "PKCS12"
                        : trustStoreType.trim();

        this.verifySsl = verifySsl;

        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3));

        if (!this.verifySsl) {

            log.warn(
                    "[DOTNET-HTTP][INIT] SSL verification is DISABLED"
            );

            builder.sslContext(createInsecureSslContext());

        } else if (this.trustStorePath != null) {

            log.info(
                    "[DOTNET-HTTP][INIT] SSL verification enabled trustStore={} type={}",
                    this.trustStorePath,
                    this.trustStoreType
            );

            builder.sslContext(
                    buildSslContext(
                            this.trustStorePath,
                            this.trustStorePassword,
                            this.trustStoreType
                    )
            );

        } else {
            log.info(
                    "[DOTNET-HTTP][INIT] SSL verification enabled using default JVM truststore"
            );
        }

        this.http = builder.build();

        log.info(
                "[DOTNET-HTTP][INIT] client created url={} requestTimeoutMs={} connectTimeoutMs={} verifySsl={} jwtConfigured={}",
                this.url,
                this.requestTimeout.toMillis(),
                3000,
                this.verifySsl,
                this.jwt != null && !this.jwt.isBlank()
        );
    }

    public ProcessingResult process(MessageRecord in) {

        if (in == null) {
            throw new IllegalArgumentException("MessageRecord is null");
        }

        String eventId =
                in.eventId() == null ? "unknown" : in.eventId();

        if (in.payload == null || in.payload.isBlank()) {
            log.warn(
                    "[DOTNET][eventId={}] payload is empty -> skip external processing",
                    eventId
            );
            return null;
        }

        if (url == null || url.isBlank()) {
            log.error(
                    "[DOTNET][eventId={}] handler.dotnet.url is not configured",
                    eventId
            );

            throw new IllegalStateException(
                    "handler.dotnet.url must be provided when runtime handler=dotnet"
            );
        }

        if (jwt == null || jwt.isBlank()) {
            log.error(
                    "[DOTNET][eventId={}] dotnet JWT is not configured",
                    eventId
            );

            throw new IllegalStateException(
                    "dotnetJwt must be provided in Vault when runtime handler=dotnet"
            );
        }

        /*
         * Время, когда сообщение реально дошло
         * до DotnetHandlerClient.process(...).
         */
        long dotnetProcessStartDttm = currentTimestampMs();

        log.info(
                "[DOTNET][eventId={}] processing START url={} payloadSize={} createdDttm={} readedDttm={}",
                eventId,
                url,
                in.payload.length(),
                in.createdDttm,
                in.readedDttm
        );

        DotnetHttpResponse httpResponse =
                call(in.payload, eventId);

        log.info(
                "[DOTNET][eventId={}] HTTP call SUCCESS status=2xx latencyMs={} responseSize={}",
                eventId,
                httpResponse.latencyMs(),
                httpResponse.body() == null
                        ? 0
                        : httpResponse.body().length()
        );

        DotnetHandlerResponse response = parseResponse(
                httpResponse.body(),
                in.createdDttm,
                in.readedDttm,
                dotnetProcessStartDttm,
                httpResponse.requestStartDttm(),
                httpResponse.requestEndDttm(),
                httpResponse.latencyMs(),
                eventId
        );

        if (response.shortJson() == null
                || response.shortJson().isBlank()) {

            log.warn(
                    "[DOTNET][eventId={}] parsed response contains empty answer -> return null",
                    eventId
            );

            return null;
        }

        if (in.mqMessageId != null) {

            log.info(
                    "[DOTNET][eventId={}] building MQ ProcessingResult mqMessageIdPresent=true",
                    eventId
            );

            ProcessingResult result = ProcessingResult.forMq(
                    in.mqMessageId,
                    response.shortJson(),
                    response.detailJson(),
                    in.payload
            );

            log.info(
                    "[DOTNET][eventId={}] processing SUCCESS transport=MQ",
                    eventId
            );

            return result;
        }

        if (in.jmsMessageId != null
                && !in.jmsMessageId.isBlank()) {

            log.info(
                    "[DOTNET][eventId={}] building JMS ProcessingResult jmsMessageId={}",
                    eventId,
                    in.jmsMessageId
            );

            ProcessingResult result = ProcessingResult.forJms(
                    in.jmsMessageId,
                    response.shortJson(),
                    response.detailJson(),
                    in.payload
            );

            log.info(
                    "[DOTNET][eventId={}] processing SUCCESS transport=JMS",
                    eventId
            );

            return result;
        }

        log.error(
                "[DOTNET][eventId={}] cannot build ProcessingResult: neither MQ nor JMS message id exists",
                eventId
        );

        throw new IllegalStateException(
                "MessageRecord has neither mqMessageId nor jmsMessageId. "
                        + "Cannot build dotnet ProcessingResult. "
                        + in
        );
    }

    private DotnetHandlerResponse parseResponse(
            String responseJson,
            Long createdDttm,
            Long readedDttm,
            long dotnetProcessStartDttm,
            long requestStartDttm,
            long requestEndDttm,
            long requestLatencyMs,
            String eventId
    ) {

        if (responseJson == null || responseJson.isBlank()) {

            log.warn(
                    "[DOTNET-PARSE][eventId={}] HTTP response body is empty",
                    eventId
            );

            return new DotnetHandlerResponse(
                    null,
                    null
            );
        }

        log.info(
                "[DOTNET-PARSE][eventId={}] parsing response START responseSize={}",
                eventId,
                responseJson.length()
        );

        try {
            JsonNode root =
                    mapper.readTree(responseJson);

            JsonNode answer =
                    root.get("answer");

            JsonNode detailAnswer =
                    root.get("detail_answer");

            if (detailAnswer == null
                    || detailAnswer.isNull()) {

                detailAnswer =
                        root.get("detailAnswer");
            }

            if (answer == null
                    || answer.isNull()) {

                log.error(
                        "[DOTNET-PARSE][eventId={}] required field 'answer' is missing",
                        eventId
                );

                throw new IllegalArgumentException(
                        "Dotnet handler response does not contain required field 'answer'"
                );
            }

            ObjectNode answerObject =
                    toObjectNode(answer);

            ObjectNode detailAnswerObject;

            if (detailAnswer == null
                    || detailAnswer.isNull()) {

                log.warn(
                        "[DOTNET-PARSE][eventId={}] detail_answer is missing -> using empty object",
                        eventId
                );

                detailAnswerObject =
                        mapper.createObjectNode();

            } else {
                detailAnswerObject =
                        toObjectNode(detailAnswer);
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

            String shortJson =
                    mapper.writeValueAsString(answerObject);

            String detailJson =
                    mapper.writeValueAsString(detailAnswerObject);

            log.info(
                    "[DOTNET-PARSE][eventId={}] parsing SUCCESS answerSize={} detailAnswerSize={}",
                    eventId,
                    shortJson.length(),
                    detailJson.length()
            );

            return new DotnetHandlerResponse(
                    shortJson,
                    detailJson
            );

        } catch (Exception e) {

            log.error(
                    "[DOTNET-PARSE][eventId={}] parsing FAILED responseSize={} exception={} message={}",
                    eventId,
                    responseJson.length(),
                    e.getClass().getSimpleName(),
                    e.getMessage()
            );

            throw new RuntimeException(
                    "Failed to parse dotnet handler response. "
                            + "Expected JSON: "
                            + "{\"answer\": ObjectNode, \"detail_answer\": ObjectNode}. "
                            + "Body="
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
            detailAnswerObject.put(
                    FIELD_DFW_CREATED_DTTM,
                    createdDttm
            );
        }

        if (readedDttm != null) {
            detailAnswerObject.put(
                    FIELD_DFW_READED_DTTM,
                    readedDttm
            );
        }

        detailAnswerObject.put(
                FIELD_DFW_DOTNET_PROCESS_START_DTTM,
                dotnetProcessStartDttm
        );

        detailAnswerObject.put(
                FIELD_DFW_REQUEST_START_DTTM,
                requestStartDttm
        );

        detailAnswerObject.put(
                FIELD_DFW_REQUEST_END_DTTM,
                requestEndDttm
        );

        detailAnswerObject.put(
                FIELD_DFW_REQUEST_LATENCY,
                requestLatencyMs
        );

        /*
         * Максимально близкое к завершению обработки
         * в DotnetHandlerClient.
         */
        long processDttm =
                currentTimestampMs();

        detailAnswerObject.put(
                FIELD_DFW_PROCESS_DTTM,
                processDttm
        );

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

        ObjectNode objectNode =
                mapper.createObjectNode();

        objectNode.set(
                "value",
                node
        );

        return objectNode;
    }

    private DotnetHttpResponse call(
            String payload,
            String eventId
    ) {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(requestTimeout)
                .header(
                        "Content-Type",
                        "application/json; charset=utf-8"
                )
                .header(
                        "Authorization",
                        "Bearer " + jwt
                )
                .POST(
                        HttpRequest.BodyPublishers.ofString(
                                payload,
                                StandardCharsets.UTF_8
                        )
                )
                .build();

        long requestStartDttm =
                currentTimestampMs();

        long startedAtNs =
                System.nanoTime();

        log.info(
                "[DOTNET-HTTP][eventId={}] POST START url={} timeoutMs={} payloadSize={}",
                eventId,
                url,
                requestTimeout.toMillis(),
                payload == null ? 0 : payload.length()
        );

        try {

            HttpResponse<String> response =
                    http.send(
                            request,
                            HttpResponse.BodyHandlers.ofString(
                                    StandardCharsets.UTF_8
                            )
                    );

            long requestEndDttm =
                    currentTimestampMs();

            long latencyMs =
                    elapsedMs(startedAtNs);

            int statusCode =
                    response.statusCode();

            int responseSize =
                    response.body() == null
                            ? 0
                            : response.body().length();

            log.info(
                    "[DOTNET-HTTP][eventId={}] RESPONSE RECEIVED url={} status={} latencyMs={} responseSize={}",
                    eventId,
                    url,
                    statusCode,
                    latencyMs,
                    responseSize
            );

            if (statusCode / 100 != 2) {

                log.error(
                        "[DOTNET-HTTP][eventId={}] HTTP FAILED url={} status={} latencyMs={} responseBody={}",
                        eventId,
                        url,
                        statusCode,
                        latencyMs,
                        truncate(response.body(), 800)
                );

                throw new RuntimeException(
                        "Dotnet handler HTTP "
                                + statusCode
                                + " for "
                                + url
                                + ", latencyMs="
                                + latencyMs
                                + ": "
                                + truncate(
                                response.body(),
                                800
                        )
                );
            }

            log.info(
                    "[DOTNET-HTTP][eventId={}] POST SUCCESS url={} status={} latencyMs={}",
                    eventId,
                    url,
                    statusCode,
                    latencyMs
            );

            return new DotnetHttpResponse(
                    response.body(),
                    latencyMs,
                    requestStartDttm,
                    requestEndDttm
            );

        } catch (Exception e) {

            long requestEndDttm =
                    currentTimestampMs();

            long latencyMs =
                    elapsedMs(startedAtNs);

            log.error(
                    "[DOTNET-HTTP][eventId={}] REQUEST FAILED url={} latencyMs={} exception={} message={}",
                    eventId,
                    url,
                    latencyMs,
                    e.getClass().getSimpleName(),
                    e.getMessage()
            );

            throw new RuntimeException(
                    "Failed to call dotnet handler API: "
                            + url
                            + ", requestStartDttm="
                            + requestStartDttm
                            + ", requestEndDttm="
                            + requestEndDttm
                            + ", latencyMs="
                            + latencyMs,
                    e
            );
        }
    }

    private static String normalizeUrlOrNull(
            String url
    ) {

        if (url == null || url.isBlank()) {
            return null;
        }

        return url.trim();
    }

    private static String normalizeJwt(
            String jwt
    ) {

        if (jwt == null || jwt.isBlank()) {
            return null;
        }

        String value =
                jwt.trim();

        if (value.regionMatches(
                true,
                0,
                "Bearer ",
                0,
                "Bearer ".length()
        )) {
            return value
                    .substring("Bearer ".length())
                    .trim();
        }

        return value;
    }

    private static String truncate(
            String s,
            int max
    ) {

        if (s == null) {
            return "";
        }

        return s.length() <= max
                ? s
                : s.substring(0, max) + "...";
    }

    private static SSLContext buildSslContext(
            String trustStorePath,
            String trustStorePassword,
            String trustStoreType
    ) {

        try {

            KeyStore trustStore =
                    KeyStore.getInstance(
                            trustStoreType == null
                                    || trustStoreType.isBlank()
                                    ? "PKCS12"
                                    : trustStoreType
                    );

            try (InputStream in =
                         Files.newInputStream(
                                 Path.of(trustStorePath)
                         )) {

                trustStore.load(
                        in,
                        trustStorePassword == null
                                ? null
                                : trustStorePassword.toCharArray()
                );
            }

            TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(
                            TrustManagerFactory.getDefaultAlgorithm()
                    );

            tmf.init(trustStore);

            SSLContext sslContext =
                    SSLContext.getInstance("TLS");

            sslContext.init(
                    null,
                    tmf.getTrustManagers(),
                    null
            );

            return sslContext;

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to build SSLContext for dotnet handler. "
                            + "trustStore="
                            + trustStorePath
                            + ", trustStoreType="
                            + trustStoreType,
                    e
            );
        }
    }

    /**
     * Небезопасный SSLContext:
     * доверяет любому сертификату.
     *
     * Использовать только для dev/test.
     */
    private static SSLContext createInsecureSslContext() {

        try {

            TrustManager[] trustAllCerts =
                    new TrustManager[]{
                            new X509TrustManager() {

                                @Override
                                public void checkClientTrusted(
                                        X509Certificate[] chain,
                                        String authType
                                ) {
                                    // trust all
                                }

                                @Override
                                public void checkServerTrusted(
                                        X509Certificate[] chain,
                                        String authType
                                ) {
                                    // trust all
                                }

                                @Override
                                public X509Certificate[] getAcceptedIssuers() {
                                    return new X509Certificate[0];
                                }
                            }
                    };

            SSLContext sslContext =
                    SSLContext.getInstance("TLS");

            sslContext.init(
                    null,
                    trustAllCerts,
                    new java.security.SecureRandom()
            );

            return sslContext;

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to create insecure SSL context",
                    e
            );
        }
    }

    private static long currentTimestampMs() {
        return Instant.now().toEpochMilli();
    }

    private static long elapsedMs(
            long startedAtNs
    ) {
        return (
                System.nanoTime()
                        - startedAtNs
        ) / 1_000_000L;
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