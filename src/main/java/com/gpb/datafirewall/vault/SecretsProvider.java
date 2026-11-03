package com.gpb.datafirewall.vault;

import com.gpb.datafirewall.vault.dto.VaultSecretsDto;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the same secrets DTO either from Vault KV v2 or from job.properties.
 * No silent fallback is performed when Vault is enabled but unavailable.
 */
public final class SecretsProvider {

    private static final Logger log = LoggerFactory.getLogger(SecretsProvider.class);
    private static final String PROPERTY_PREFIX = "secrets.";

    private SecretsProvider() {
    }

    public static VaultSecretsDto load(ParameterTool pt) {
        Objects.requireNonNull(pt, "job parameters");
        if (pt.getBoolean("vault.enabled", true)) {
            log.info("[SECRETS] vault.enabled=true; loading credentials from HashiCorp Vault");
            return VaultClient.loadSecrets(VaultClientConfig.fromArgs(pt));
        }

        log.warn("[SECRETS] vault.enabled=false; credentials are loaded from job.properties. "
                + "Restrict file permissions and do not commit real credentials to source control.");
        return fromProperties(pt);
    }

    /**
     * Only invoked in local-properties mode. Field names deliberately match VaultSecretsDto.
     * All previously mandatory Vault fields remain mandatory here; dotnetJwt stays optional.
     */
    static VaultSecretsDto fromProperties(ParameterTool pt) {
        try {
            return new VaultSecretsDto(
                    pt.get(PROPERTY_PREFIX + "keystorePassword", null),
                    pt.get(PROPERTY_PREFIX + "truststorePassword", null),
                    pt.get(PROPERTY_PREFIX + "mqUser", null),
                    pt.get(PROPERTY_PREFIX + "mqPassword", null),
                    pt.get(PROPERTY_PREFIX + "mqKeystorePassword", null),
                    pt.get(PROPERTY_PREFIX + "mqTruststorePassword", null),
                    pt.get(PROPERTY_PREFIX + "kafkaUser", null),
                    pt.get(PROPERTY_PREFIX + "kafkaPassword", null),
                    pt.get(PROPERTY_PREFIX + "jwt", null),
                    pt.get(PROPERTY_PREFIX + "dotnetJwt", null));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "vault.enabled=false: invalid secrets.* properties: " + e.getMessage(), e);
        }
    }

    /**
     * Avoid exposing locally configured credentials in Flink's GlobalJobParameters
     * and serializing their values again inside the Kafka cache source.
     * Credentials needed by workers travel in the existing VaultSecretsDto object.
     */
    public static ParameterTool withoutLocalSecrets(ParameterTool pt) {
        Objects.requireNonNull(pt, "job parameters");
        Map<String, String> nonSensitive = new HashMap<>(pt.toMap());
        nonSensitive.keySet().removeIf(key -> key.startsWith(PROPERTY_PREFIX));
        return ParameterTool.fromMap(nonSensitive);
    }
}
