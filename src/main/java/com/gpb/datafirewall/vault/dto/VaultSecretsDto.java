package com.gpb.datafirewall.vault.dto;

import java.io.Serial;
import java.io.Serializable;

public record VaultSecretsDto(
        String keystorePassword,
        String truststorePassword,
        String mqUser,
        String mqPassword,
        String mqKeystorePassword,
        String mqTruststorePassword
) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public VaultSecretsDto {
        requireNotBlank(keystorePassword, "keystorePassword");
        requireNotBlank(truststorePassword, "truststorePassword");
        requireNotBlank(mqUser, "mqUser");
        requireNotBlank(mqPassword, "mqPassword");
        requireNotBlank(mqPassword, "mqKeystorePassword");
        requireNotBlank(mqPassword, "mqTruststorePassword");
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Vault secret field is missing or blank: " + name);
        }
    }
}