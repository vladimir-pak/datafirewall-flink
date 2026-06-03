package com.gpb.datafirewall.services;

import com.gpb.datafirewall.dto.ProcessingResult;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public final class DotnetHandlerClient {

    private final HttpClient http;
    private final String url;
    private final String jwt;
    private final Duration requestTimeout;

    public DotnetHandlerClient(String url, String jwt, long timeoutMs) {
        this.url = normalizeUrlOrNull(url);
        this.jwt = normalizeJwt(jwt);
        this.requestTimeout = Duration.ofMillis(timeoutMs <= 0 ? 20_000L : timeoutMs);
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
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

        String responseJson = call(in.payload);
        if (responseJson == null || responseJson.isBlank()) {
            return null;
        }

        if (in.mqMessageId != null) {
            return ProcessingResult.forMq(
                    in.mqMessageId,
                    responseJson,
                    null,
                    in.payload
            );
        }

        if (in.jmsMessageId != null && !in.jmsMessageId.isBlank()) {
            return ProcessingResult.forJms(
                    in.jmsMessageId,
                    responseJson,
                    null,
                    in.payload
            );
        }

        throw new IllegalStateException(
                "MessageRecord has neither mqMessageId nor jmsMessageId. Cannot build dotnet ProcessingResult. " + in
        );
    }

    private String call(String payload) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));

        if (jwt != null && !jwt.isBlank()) {
            builder.header("Authorization", "Bearer " + jwt);
        }

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
}
