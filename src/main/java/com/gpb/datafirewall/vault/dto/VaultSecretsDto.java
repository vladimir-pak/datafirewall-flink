package com.gpb.datafirewall.vault.dto;

import java.io.Serial;
import java.io.Serializable;

public record VaultSecretsDto(
        String keystorePassword,
        String truststorePassword,
        String mqUser,
        String mqPassword,
        String mqKeystorePassword,
        String mqTruststorePassword,
        String kafkaUser,
        String kafkaPassword,
        String jwt,
        String dotnetJwt
) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public VaultSecretsDto {
        requireNotBlank(keystorePassword, "keystorePassword");
        requireNotBlank(truststorePassword, "truststorePassword");
        requireNotBlank(mqUser, "mqUser");
        requireNotBlank(mqPassword, "mqPassword");
        requireNotBlank(mqKeystorePassword, "mqKeystorePassword");
        requireNotBlank(mqTruststorePassword, "mqTruststorePassword");
        requireNotBlank(kafkaUser, "kafkaUser");
        requireNotBlank(kafkaPassword, "kafkaPassword");
        requireNotBlank(jwt, "jwt");
        dotnetJwt = trimToNull(dotnetJwt);
    }

    public String requireDotnetJwt() {
        requireNotBlank(dotnetJwt, "dotnetJwt");
        return dotnetJwt;
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Vault secret field is missing or blank: " + name);
        }
    }

    private static String trimToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
