package com.gpb.datafirewall.vault.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record VaultAppRoleLoginRequestDto(
        @JsonProperty("role_id")
        String roleId,

        @JsonProperty("secret_id")
        String secretId
) {
}