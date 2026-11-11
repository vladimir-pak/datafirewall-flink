package com.gpb.datafirewall.vault;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gpb.datafirewall.vault.dto.VaultSecretsDto;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

class SecretsProviderTest {

    private static Map<String, String> sampleSecrets() {
        Map<String, String> values = new HashMap<>();
        values.put("vault.enabled", "false");
        values.put("secrets.keystorePassword", "key-store-pass");
        values.put("secrets.truststorePassword", "trust-store-pass");
        values.put("secrets.mqUser", "mq-user");
        values.put("secrets.mqPassword", "mq-pass");
        values.put("secrets.mqKeystorePassword", "mq-key-store-pass");
        values.put("secrets.mqTruststorePassword", "mq-trust-store-pass");
        values.put("secrets.kafkaUser", "kafka-user");
        values.put("secrets.kafkaPassword", "kafka-pass");
        values.put("secrets.jwt", "api-jwt");
        values.put("secrets.dotnetJwt", "dotnet-jwt");
        return values;
    }

    @Test
    void disabledVaultLoadsAllSecretsWithoutVaultConfiguration() {
        VaultSecretsDto secrets = SecretsProvider.load(ParameterTool.fromMap(sampleSecrets()));
        assertEquals("key-store-pass", secrets.keystorePassword());
        assertEquals("trust-store-pass", secrets.truststorePassword());
        assertEquals("mq-user", secrets.mqUser());
        assertEquals("mq-pass", secrets.mqPassword());
        assertEquals("mq-key-store-pass", secrets.mqKeystorePassword());
        assertEquals("mq-trust-store-pass", secrets.mqTruststorePassword());
        assertEquals("kafka-user", secrets.kafkaUser());
        assertEquals("kafka-pass", secrets.kafkaPassword());
        assertEquals("api-jwt", secrets.jwt());
        assertEquals("dotnet-jwt", secrets.dotnetJwt());
    }

    @Test
    void disabledVaultPermitsAbsentDotnetJwt() {
        Map<String, String> values = sampleSecrets();
        values.remove("secrets.dotnetJwt");
        assertNull(SecretsProvider.load(ParameterTool.fromMap(values)).dotnetJwt());
    }

    @Test
    void disabledVaultFailsOnMissingMandatorySecret() {
        Map<String, String> values = sampleSecrets();
        values.remove("secrets.kafkaPassword");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> SecretsProvider.load(ParameterTool.fromMap(values)));
        assertTrue(error.getMessage().contains("kafkaPassword"));
        assertFalse(error.getMessage().contains("kafka-pass"));
    }

    @Test
    void enabledVaultStillRequiresVaultConfiguration() {
        Map<String, String> values = sampleSecrets();
        values.put("vault.enabled", "true");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> SecretsProvider.load(ParameterTool.fromMap(values)));
        assertTrue(error.getMessage().contains("vault.url"));
    }

    @Test
    void secretsNeverAppearInGlobalJobParameters() {
        Map<String, String> values = sampleSecrets();
        values.put("mq.host", "localhost");
        ParameterTool publicParameters = SecretsProvider.withoutLocalSecrets(
                ParameterTool.fromMap(values));
        assertEquals("localhost", publicParameters.get("mq.host"));
        assertEquals("false", publicParameters.get("vault.enabled"));
        assertFalse(publicParameters.toMap().keySet().stream()
                .anyMatch(key -> key.startsWith("secrets.")));
    }
}
