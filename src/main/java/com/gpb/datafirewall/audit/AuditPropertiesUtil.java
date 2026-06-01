package com.gpb.datafirewall.audit;

import org.apache.flink.api.java.utils.ParameterTool;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class AuditPropertiesUtil {

    private static final Set<String> SENSITIVE_PARTS = Set.of(
            "password", "secret", "token", "keytab", "jaas", "credential"
    );

    private AuditPropertiesUtil() {
    }

    public static Map<String, String> maskedProperties(ParameterTool pt) {
        if (pt == null) {
            return Map.of();
        }
        return pt.toMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> isSensitive(e.getKey()) ? "***" : String.valueOf(e.getValue()),
                        (a, b) -> b,
                        LinkedHashMap::new
                ));
    }

    public static String sha256OfMaskedProperties(ParameterTool pt) {
        Map<String, String> props = maskedProperties(pt);
        String canonical = props.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("\n"));
        return sha256(canonical);
    }

    public static String detectPreviousHashAndStore(String stateFile, String currentHash) {
        if (stateFile == null || stateFile.isBlank() || currentHash == null || currentHash.isBlank()) {
            return null;
        }
        try {
            Path path = Path.of(stateFile).toAbsolutePath().normalize();
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            String previous = Files.exists(path)
                    ? Files.readString(path, StandardCharsets.UTF_8).trim()
                    : null;

            Files.writeString(path, currentHash, StandardCharsets.UTF_8);
            return previous == null || previous.isBlank() ? null : previous;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isSensitive(String key) {
        if (key == null) {
            return false;
        }
        String k = key.toLowerCase(java.util.Locale.ROOT);
        return SENSITIVE_PARTS.stream().anyMatch(k::contains);
    }

    private static String sha256(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format(java.util.Locale.ROOT, "%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
