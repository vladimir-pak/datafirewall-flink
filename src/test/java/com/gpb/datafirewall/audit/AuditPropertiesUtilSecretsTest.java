package com.gpb.datafirewall.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

class AuditPropertiesUtilSecretsTest {

    @Test
    void masksJwtAndLocallyConfiguredCredentials() {
        ParameterTool pt = ParameterTool.fromMap(Map.of(
                "secrets.jwt", "top-secret-api-token",
                "secrets.dotnetJwt", "top-secret-dotnet-token",
                "secrets.mqPassword", "top-secret-mq-pass",
                "handler.dotnet.jwt", "legacy-secret-jwt",
                "mq.host", "localhost"));
        Map<String, String> masked = AuditPropertiesUtil.maskedProperties(pt);
        assertEquals("***", masked.get("secrets.jwt"));
        assertEquals("***", masked.get("secrets.dotnetJwt"));
        assertEquals("***", masked.get("secrets.mqPassword"));
        assertEquals("***", masked.get("handler.dotnet.jwt"));
        assertEquals("localhost", masked.get("mq.host"));
        assertFalse(masked.toString().contains("top-secret"));
        assertFalse(masked.toString().contains("legacy-secret"));
    }
}
