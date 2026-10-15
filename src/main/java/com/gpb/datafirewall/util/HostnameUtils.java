package com.gpb.datafirewall.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class HostnameUtils {

    private static final String HOSTNAME = resolveHostname();

    private HostnameUtils() {
    }

    public static String getHostname() {
        return HOSTNAME;
    }

    private static String resolveHostname() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();

            if (hostname != null && !hostname.isBlank()) {
                return hostname;
            }
        } catch (UnknownHostException ignored) {
            // Fallback на переменную окружения
        }

        String hostname = System.getenv("HOSTNAME");

        if (hostname != null && !hostname.isBlank()) {
            return hostname;
        }

        return "UNKNOWN_HOST";
    }
}