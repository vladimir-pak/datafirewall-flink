package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.Duration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public final class DotnetHandlerClient {

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final String url;
    private final String jwt;
    private final Duration requestTimeout;

    private final String trustStorePath;
    private final String trustStorePassword;
    private final String trustStoreType;

    public DotnetHandlerClient(
            String url,
            String jwt,
            long timeoutMs,
            String trustStorePath,
            String trustStorePassword,
            String trustStoreType
    ) {
        this(url, jwt, timeoutMs, new ObjectMapper(), trustStorePath, trustStorePassword, trustStoreType);
    }

    public DotnetHandlerClient(
            String url,
            String jwt,
            long timeoutMs,
            ObjectMapper mapper,
            String trustStorePath,
            String trustStorePassword,
            String trustStoreType
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

        if (this.trustStorePath != null) {
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

        DotnetHandlerResponse response = parseResponse(call(in.payload));
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

    private DotnetHandlerResponse parseResponse(String responseJson) {
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

            String shortJson = mapper.writeValueAsString(answer);
            String detailJson = detailAnswer == null || detailAnswer.isNull()
                    ? null
                    : mapper.writeValueAsString(detailAnswer);

            return new DotnetHandlerResponse(shortJson, detailJson);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to parse dotnet handler response. Expected JSON: {\"answer\": ObjectNode, \"detail_answer\": ObjectNode}. Body="
                            + truncate(responseJson, 800),
                    e
            );
        }
    }

    private String call(String payload) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Authorization", "Bearer " + jwt)
                .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));

        HttpRequest request = builder.build();

        try {
            HttpResponse<String> response = http.send(
                    request,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            );

            if (response.statusCode() / 100 != 2) {
                throw new RuntimeException(
                        "Dotnet handler HTTP " + response.statusCode() +
                                " for " + url + ": " + truncate(response.body(), 800)
                );
            }

            return response.body();
        } catch (Exception e) {
            throw new RuntimeException("Failed to call dotnet handler API: " + url, e);
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

    private record DotnetHandlerResponse(String shortJson, String detailJson) {
    }
}
