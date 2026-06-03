package com.gpb.datafirewall.services;

import java.io.Serializable;
import java.util.Locale;

public enum DynamicHandler implements Serializable {
    FLINK("flink"),
    DOTNET("dotnet");

    private final String value;

    DynamicHandler(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static DynamicHandler from(String value, DynamicHandler defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue == null ? FLINK : defaultValue;
        }

        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "flink" -> FLINK;
            case "dotnet" -> DOTNET;
            default -> throw new IllegalArgumentException(
                    "Unsupported handler=" + value + ". Supported: flink|dotnet"
            );
        };
    }
}
