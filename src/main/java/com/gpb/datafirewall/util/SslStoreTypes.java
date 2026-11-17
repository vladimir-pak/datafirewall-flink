package com.gpb.datafirewall.util;

import java.util.Locale;

/** Java keystore types; filename extensions are accepted as aliases, not auto-detected. */
public final class SslStoreTypes {
    private SslStoreTypes() { }

    public static String normalize(String value) {
        if (value == null || value.isBlank()) return "JKS";
        String type = value.trim();
        return switch (type.toUpperCase(Locale.ROOT)) {
            case "P12", "PFX", "PKCS12", "PKCS#12" -> "PKCS12";
            case "JKS" -> "JKS";
            case "JCEKS" -> "JCEKS";
            default -> type; // Other types require an installed security provider.
        };
    }
}