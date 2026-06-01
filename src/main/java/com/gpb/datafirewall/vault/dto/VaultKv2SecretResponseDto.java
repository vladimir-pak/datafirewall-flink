package com.gpb.datafirewall.vault.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record VaultKv2SecretResponseDto(
        DataWrapperDto data
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DataWrapperDto(
            SecretDataDto data
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SecretDataDto(
            String keystorePassword,
            String truststorePassword,
            String mqUser,
            String mqPassword,
            String mqKeystorePassword,
            String mqTrustStorePassword,
            String kafkaUser,
            String kafkaPassword
    ) {
    }

    public VaultSecretsDto toSecretsDto() {
        if (data == null || data.data == null) {
            throw new IllegalStateException("Vault KV v2 response does not contain data.data object");
        }

        return new VaultSecretsDto(
                data.data.keystorePassword(),
                data.data.truststorePassword(),
                data.data.mqUser(),
                data.data.mqPassword(),
                data.data.mqKeystorePassword(),
                data.data.mqTrustStorePassword(),
                data.data.kafkaUser(),
                data.data.kafkaPassword()
        );
    }
}