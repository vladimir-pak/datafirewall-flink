package com.gpb.datafirewall.vault.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record VaultAppRoleLoginResponseDto(
        AuthDto auth
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AuthDto(
            @JsonProperty("client_token")
            String clientToken
    ) {
    }

    public String requireClientToken() {
        if (auth == null || auth.clientToken == null || auth.clientToken.isBlank()) {
            throw new IllegalStateException("Vault AppRole login response does not contain auth.client_token");
        }
        return auth.clientToken;
    }
}