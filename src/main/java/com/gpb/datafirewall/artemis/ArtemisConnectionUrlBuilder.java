package com.gpb.datafirewall.artemis;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public final class ArtemisConnectionUrlBuilder {

    private ArtemisConnectionUrlBuilder() {
    }

    public static String buildFromBrokerUrl(
            String brokerUrl,
            boolean tlsEnabled,
            String trustStorePath,
            String trustStorePassword,
            String keyStorePath,
            String keyStorePassword,
            String cipherSuites
    ) {
        if (brokerUrl == null || brokerUrl.isBlank()) {
            throw new IllegalArgumentException("artemis.broker.url must be provided");
        }

        String cleanUrl = brokerUrl.trim();

        if (!tlsEnabled) {
            return cleanUrl;
        }

        if (cleanUrl.contains("?")) {
            return cleanUrl;
        }

        StringBuilder sb = new StringBuilder(cleanUrl);
        sb.append("?sslEnabled=true");
        sb.append("&protocols=TLSv1.2,TLSv1.3");
        sb.append("&ha=true");

        if (notBlank(cipherSuites)) {
            sb.append("&enabledCipherSuites=").append(cipherSuites);
        }
        if (notBlank(trustStorePath)) {
            sb.append("&trustStorePath=").append(trustStorePath);
        }
        if (notBlank(trustStorePassword)) {
            sb.append("&trustStorePassword=").append(urlEncode(trustStorePassword));
        }
        if (notBlank(keyStorePath)) {
            sb.append("&keyStorePath=").append(keyStorePath);
        }
        if (notBlank(keyStorePassword)) {
            sb.append("&keyStorePassword=").append(urlEncode(keyStorePassword));
        }

        return sb.toString();
    }

    public static String build(
            String host,
            int port,
            boolean tlsEnabled,
            String trustStorePath,
            String trustStorePassword,
            String keyStorePath,
            String keyStorePassword,
            String cipherSuites
    ) {
        String brokerUrl = "tcp://" + host + ":" + port;
        return buildFromBrokerUrl(
                brokerUrl,
                tlsEnabled,
                trustStorePath,
                trustStorePassword,
                keyStorePath,
                keyStorePassword,
                cipherSuites
        );
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}