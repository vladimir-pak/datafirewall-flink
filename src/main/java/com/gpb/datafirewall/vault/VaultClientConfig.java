package com.gpb.datafirewall.vault;

import org.apache.flink.api.java.utils.ParameterTool;

import java.nio.file.Path;

public record VaultClientConfig(
        String url,
        Path appRoleConfPath,
        String authMount,
        String kvMount,
        String secretPath,
        String namespace,
        int connectTimeoutMs,
        int requestTimeoutMs
) {

    public static VaultClientConfig fromArgs(ParameterTool pt) {
        String url = require(pt.get("vault.url", null), "vault.url");
        String appRoleConf = require(pt.get("vault.approle.conf", null), "vault.approle.conf");

        String authMount = normalizePath(pt.get("vault.auth.mount", "approle"));
        String kvMount = normalizePath(pt.get("vault.kv.mount", "secret"));
        String secretPath = normalizePath(require(pt.get("vault.secret.path", null), "vault.secret.path"));

        String namespace = blankToNull(pt.get("vault.namespace", null));

        int connectTimeoutMs = pt.getInt("vault.connect.timeout.ms", 3000);
        int requestTimeoutMs = pt.getInt("vault.request.timeout.ms", 5000);

        return new VaultClientConfig(
                trimTrailingSlash(url),
                Path.of(appRoleConf),
                authMount,
                kvMount,
                secretPath,
                namespace,
                connectTimeoutMs,
                requestTimeoutMs
        );
    }

    private static String require(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be provided");
        }
        return value.trim();
    }

    private static String blankToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static String trimTrailingSlash(String value) {
        String result = value.trim();
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static String normalizePath(String value) {
        String result = require(value, "path");

        while (result.startsWith("/")) {
            result = result.substring(1);
        }

        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }

        return result;
    }
}