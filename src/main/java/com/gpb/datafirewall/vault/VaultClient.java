package com.gpb.datafirewall.vault;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.vault.dto.VaultAppRoleLoginRequestDto;
import com.gpb.datafirewall.vault.dto.VaultAppRoleLoginResponseDto;
import com.gpb.datafirewall.vault.dto.VaultKv2SecretResponseDto;
import com.gpb.datafirewall.vault.dto.VaultSecretsDto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

public final class VaultClient {

    private static final Logger log = LoggerFactory.getLogger(VaultClient.class);

    private static final ObjectMapper OM = new ObjectMapper();

    private VaultClient() {
    }

    public static VaultSecretsDto loadSecrets(VaultClientConfig config) {
        try {
            HttpClient httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(config.connectTimeoutMs()))
                    .build();

            AppRoleCredentials credentials = readAppRoleCredentials(config);
            String clientToken = loginByAppRole(config, httpClient, credentials);
            VaultSecretsDto secrets = readKv2Secret(config, httpClient, clientToken);

            log.info(
                    "[VAULT] Secret successfully loaded once. kvMount={}, secretPath={}",
                    config.kvMount(),
                    config.secretPath()
            );

            return secrets;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to load secrets from Vault. secretPath=" + safeSecretPath(config),
                    e
            );
        }
    }

    private static AppRoleCredentials readAppRoleCredentials(VaultClientConfig config) throws Exception {
        Properties props = new Properties();

        try (InputStream in = java.nio.file.Files.newInputStream(config.appRoleConfPath())) {
            props.load(in);
        }

        String roleId = trimToNull(props.getProperty("role_id"));
        String secretId = trimToNull(props.getProperty("secret_id"));

        if (roleId == null) {
            throw new IllegalArgumentException("role_id is missing in " + config.appRoleConfPath());
        }

        if (secretId == null) {
            throw new IllegalArgumentException("secret_id is missing in " + config.appRoleConfPath());
        }

        return new AppRoleCredentials(roleId, secretId);
    }

    private static String loginByAppRole(
            VaultClientConfig config,
            HttpClient httpClient,
            AppRoleCredentials credentials
    ) throws Exception {
        String url = config.url()
                + "/v1/auth/"
                + encodePath(config.authMount())
                + "/login";

        VaultAppRoleLoginRequestDto requestDto = new VaultAppRoleLoginRequestDto(
                credentials.roleId(),
                credentials.secretId()
        );

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(config.requestTimeoutMs()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(OM.writeValueAsString(requestDto)));

        addNamespaceHeader(requestBuilder, config);

        HttpResponse<String> response = httpClient.send(
                requestBuilder.build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
        );

        require2xx(response, "Vault AppRole login");

        VaultAppRoleLoginResponseDto responseDto = OM.readValue(
                response.body(),
                VaultAppRoleLoginResponseDto.class
        );

        return responseDto.requireClientToken();
    }

    private static VaultSecretsDto readKv2Secret(
            VaultClientConfig config,
            HttpClient httpClient,
            String clientToken
    ) throws Exception {
        String url = config.url()
                + "/v1/"
                + encodePath(config.kvMount())
                + "/data/"
                + encodePath(config.secretPath());

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(config.requestTimeoutMs()))
                .header("X-Vault-Token", clientToken)
                .GET();

        addNamespaceHeader(requestBuilder, config);

        HttpResponse<String> response = httpClient.send(
                requestBuilder.build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
        );

        require2xx(response, "Vault KV v2 read secret");

        VaultKv2SecretResponseDto responseDto = OM.readValue(
                response.body(),
                VaultKv2SecretResponseDto.class
        );

        return responseDto.toSecretsDto();
    }

    private static void addNamespaceHeader(HttpRequest.Builder builder, VaultClientConfig config) {
        if (config.namespace() != null && !config.namespace().isBlank()) {
            builder.header("X-Vault-Namespace", config.namespace());
        }
    }

    private static void require2xx(HttpResponse<String> response, String operation) {
        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            return;
        }

        String body = response.body() == null ? "" : response.body();
        if (body.length() > 1000) {
            body = body.substring(0, 1000) + "...";
        }

        throw new IllegalStateException(
                operation + " failed. httpStatus=" + status + ", responseBody=" + body
        );
    }

    private static String encodePath(String path) {
        String[] parts = path.split("/");
        StringBuilder result = new StringBuilder();

        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }

            if (!result.isEmpty()) {
                result.append('/');
            }

            result.append(URLEncoder.encode(part, StandardCharsets.UTF_8));
        }

        return result.toString();
    }

    private static String trimToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static String safeSecretPath(VaultClientConfig config) {
        if (config == null) {
            return "<null>";
        }
        return config.kvMount() + "/" + config.secretPath();
    }

    private record AppRoleCredentials(
            String roleId,
            String secretId
    ) {
    }
}