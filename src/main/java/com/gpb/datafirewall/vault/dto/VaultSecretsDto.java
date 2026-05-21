package com.gpb.datafirewall.vault.dto;

public record VaultSecretsDto(
        String keystorePassword,
        String truststorePassword,
        String mqUser,
        String mqPassword
) {

    public VaultSecretsDto {
        requireNotBlank(keystorePassword, "keystorePassword");
        requireNotBlank(truststorePassword, "truststorePassword");
        requireNotBlank(mqUser, "mqUser");
        requireNotBlank(mqPassword, "mqPassword");
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Vault secret field is missing or blank: " + name);
        }
    }
}