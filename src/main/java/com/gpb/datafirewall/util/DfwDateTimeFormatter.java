package com.gpb.datafirewall.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class DfwDateTimeFormatter {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter
                    .ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'")
                    .withZone(ZoneOffset.UTC);

    private DfwDateTimeFormatter() {
    }

    public static String formatEpochMillis(Long timestamp) {
        if (timestamp == null) {
            return null;
        }

        return FORMATTER.format(
                Instant.ofEpochMilli(timestamp)
        );
    }
}
