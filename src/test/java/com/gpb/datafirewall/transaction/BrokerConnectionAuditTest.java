package com.gpb.datafirewall.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gpb.datafirewall.audit.AuditConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerConnectionAuditTest {

    @TempDir
    Path tempDirectory;

    private AuditConfig config(Path file) {
        return AuditConfig.from(ParameterTool.fromMap(Map.of(
                "cef.audit.enabled", "true",
                "cef.audit.file.enabled", "true",
                "cef.audit.kafka.enabled", "false",
                "cef.audit.cef.path", file.toString(),
                "cef.audit.host.name", "test-host",
                "cef.audit.src.ip", "127.0.0.1",
                "cef.audit.src.mac", "00:00:00:00:00:00")), null, "localhost:9092");
    }

    @Test
    void mqEmitsLegacySourceAndSinkConnectionLifecycle() throws IOException {
        Path file = tempDirectory.resolve("mq-cef.log");
        try (BrokerConnectionAudit audit = BrokerConnectionAudit.mq(config(file), 2, 8,
                "localhost", 1414, "TEST.CHANNEL", "QM1",
                "INPUT.Q", "OUTPUT.Q", "testuser", false,
                null, null, "DFW", "SERVICE")) {
            audit.connecting();
            audit.connected();
        }

        List<String> lines = Files.readAllLines(file);
        assertEquals(6, lines.size());
        assertTrue(lines.get(0).contains("IBM MQ Source Connecting"));
        assertTrue(lines.get(1).contains("IBM MQ Sink Connecting"));
        assertTrue(lines.get(2).contains("IBM MQ Source Connected"));
        assertTrue(lines.get(3).contains("IBM MQ Sink Connected"));
        assertTrue(lines.get(4).contains("IBM MQ Source Disconnected"));
        assertTrue(lines.get(5).contains("IBM MQ Sink Disconnected"));
        assertTrue(lines.get(4).contains("subtaskIndex\\=2"));
        assertTrue(lines.get(4).contains("parallelism\\=8"));
        assertTrue(lines.get(4).contains("transactional\\=true"));
    }

    @Test
    void artemisReportsFailedConnectionAndMasksPasswords() throws IOException {
        Path file = tempDirectory.resolve("artemis-cef.log");
        try (BrokerConnectionAudit audit = BrokerConnectionAudit.artemis(config(file), 1, 4,
                "tcp://localhost:61616?trustStorePassword=secret&keyStorePassword=top-secret",
                "IN.Q", "OUT.Q", "testuser", 500)) {
            audit.connecting();
            audit.connectionFailed(new IOException("connection refused"));
        }
        List<String> lines = Files.readAllLines(file);
        assertEquals(4, lines.size());
        assertTrue(lines.get(2).contains("Artemis Source Connection Failed"));
        assertTrue(lines.get(3).contains("Artemis Sink Connection Failed"));
        assertTrue(lines.get(2).contains("outcome=failure"));
        assertFalse(String.join("\n", lines).contains("top-secret"));
    }
}
