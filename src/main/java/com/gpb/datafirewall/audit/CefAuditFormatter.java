package com.gpb.datafirewall.audit;

import java.net.InetAddress;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

final class CefAuditFormatter {

    private CefAuditFormatter() {
    }

    static String format(AuditConfig config, CefAuditEvent event, long externalId) {
        AuditEventType type = event.eventType();
        String appName = nvl(config.appName(), event.jobName());
        String appVersion = nvl(config.appVersion(), event.jobVersion());
        Endpoint endpoint = resolveEndpoint(config, event);
        String user = resolveUser(config, event);
        String protocol = resolveProtocol(config, event);
        String message = buildMessage(config, event, endpoint);
        String outcome = isFailed(event.status()) ? "failure" : "success";

        StringBuilder cef = new StringBuilder(512);
        cef.append("CEF:")
                .append(AuditConfig.CEF_VERSION).append('|')
                .append(escapeHeader(AuditConfig.CEF_PROJECT)).append('|')
                .append(escapeHeader(appName)).append('|')
                .append(escapeHeader(appVersion)).append('|')
                .append(escapeHeader(type == null ? "UNKNOWN" : type.eventClass())).append('|')
                .append(escapeHeader(type == null ? "Unknown Event" : type.eventName())).append('|')
                .append(AuditConfig.CEF_SEVERITY).append('|');

        Map<String, String> ext = new LinkedHashMap<>();
        ext.put("externalId", String.valueOf(externalId));
        ext.put("suser", user);
        ext.put("sntdom", AuditConfig.CEF_DOMAIN);
        ext.put("src", config.sourceIp());
        ext.put("smac", config.sourceMac());
        ext.put("shost", config.sourceHost());
        ext.put("duser", user);
        ext.put("dntdom", AuditConfig.CEF_DOMAIN);
        ext.put("dst", endpoint.hostIp());
        ext.put("dmac", endpoint.mac());
        ext.put("dhost", endpoint.hostName());
        ext.put("spt", String.valueOf(config.sourcePort()));
        ext.put("dpt", String.valueOf(endpoint.port()));
        ext.put("app", protocol);
        ext.put("start", String.valueOf(event.eventTime().toEpochMilli()));
        ext.put("end", "");
        ext.put("msg", message);
        ext.put("deviceProcessName", appName + "-" + appVersion);
        ext.put("outcome", outcome);

        StringJoiner joiner = new StringJoiner(" ");
        ext.forEach((k, v) -> joiner.add(k + "=" + escapeExtension(v)));
        cef.append(joiner);
        return cef.toString();
    }

    private static Endpoint resolveEndpoint(AuditConfig config, CefAuditEvent event) {
        Map<String, Object> payload = event.payload();

        String host = firstNonBlank(
                string(payload.get("dst")),
                string(payload.get("host")),
                hostFromBrokerUrl(string(payload.get("brokerUrl"))),
                hostFromBootstrap(string(payload.get("bootstrap")))
        );

        int port = firstPositive(
                integer(payload.get("dpt")),
                integer(payload.get("port")),
                portFromBrokerUrl(string(payload.get("brokerUrl"))),
                portFromBootstrap(string(payload.get("bootstrap")))
        );

        if (host == null || host.isBlank()) {
            return new Endpoint(config.sourceIp(), config.sourceHost(), config.sourceMac(), config.sourcePort());
        }

        String ip = resolveIp(host);
        String hostName = host.equals(ip) ? "" : host;
        String mac = ip.equals(config.sourceIp()) ? config.sourceMac() : "";
        return new Endpoint(ip, hostName, mac, port < 0 ? config.sourcePort() : port);
    }

    private static String resolveUser(AuditConfig config, CefAuditEvent event) {
        Map<String, Object> payload = event.payload();
        return firstNonBlank(
                string(payload.get("suser")),
                string(payload.get("duser")),
                string(payload.get("connectionUser")),
                string(payload.get("user")),
                config.sourceUser()
        );
    }

    private static String resolveProtocol(AuditConfig config, CefAuditEvent event) {
        Map<String, Object> payload = event.payload();
        String explicit = firstNonBlank(
                string(payload.get("protocol")),
                string(payload.get("securityProtocol"))
        );
        if (explicit != null) {
            return explicit;
        }

        String brokerUrl = string(payload.get("brokerUrl"));
        String scheme = schemeFromBrokerUrl(brokerUrl);
        if (scheme != null) {
            if ("ssl".equalsIgnoreCase(scheme)) {
                return "SSL";
            }
            if ("tcp".equalsIgnoreCase(scheme)) {
                return AuditConfig.DEFAULT_PROTOCOL;
            }
            return scheme.toUpperCase();
        }

        if (event.eventType() != null && event.eventType().name().startsWith("KAFKA_")) {
            return config.kafkaSecurityProtocol();
        }
        if (Boolean.TRUE.equals(payload.get("tlsEnabled")) || "true".equalsIgnoreCase(string(payload.get("tlsEnabled")))) {
            return "TLS";
        }
        return AuditConfig.DEFAULT_PROTOCOL;
    }

    private static String buildMessage(AuditConfig config, CefAuditEvent event, Endpoint endpoint) {
        Map<String, Object> payload = event.payload();
        StringBuilder msg = new StringBuilder();
        AuditEventType type = event.eventType();
        msg.append(type == null ? "Audit event" : type.eventName());
        msg.append("; eventId=").append(event.eventId());
        msg.append("; clusterId=").append(nvl(event.clusterId(), config.clusterId()));
        msg.append("; jobName=").append(nvl(event.jobName(), config.jobName()));
        msg.append("; status=").append(nvl(event.status(), "SUCCESS"));
        if (event.subtaskIndex() != null) {
            msg.append("; subtaskIndex=").append(event.subtaskIndex());
        }
        if (event.parallelism() != null) {
            msg.append("; parallelism=").append(event.parallelism());
        }
        appendPayload(msg, payload, "component");
        appendPayload(msg, payload, "connectionName");
        appendPayload(msg, payload, "topic");
        appendPayload(msg, payload, "queue");
        appendPayload(msg, payload, "qmgr");
        appendPayload(msg, payload, "channel");
        appendPayload(msg, payload, "propertiesHash");
        appendPayload(msg, payload, "oldPropertiesHash");
        appendPayload(msg, payload, "newPropertiesHash");
        appendPayload(msg, payload, "durationMs");
        appendPayload(msg, payload, "errorClass");
        appendPayload(msg, payload, "errorMessage");
        msg.append("; dst=").append(endpoint.hostIp());
        if (endpoint.port() > 0) {
            msg.append(':').append(endpoint.port());
        }
        return msg.toString();
    }

    private static void appendPayload(StringBuilder msg, Map<String, Object> payload, String key) {
        String value = string(payload.get(key));
        if (value != null && !value.isBlank()) {
            msg.append("; ").append(key).append('=').append(value);
        }
    }

    private static boolean isFailed(String status) {
        return status != null && ("FAILED".equalsIgnoreCase(status) || "ERROR".equalsIgnoreCase(status));
    }

    private static String hostFromBrokerUrl(String url) {
        try {
            if (url == null || url.isBlank()) {
                return null;
            }
            URI uri = URI.create(firstUrlPart(url));
            return uri.getHost();
        } catch (Exception e) {
            return null;
        }
    }

    private static int portFromBrokerUrl(String url) {
        try {
            if (url == null || url.isBlank()) {
                return -1;
            }
            URI uri = URI.create(firstUrlPart(url));
            return uri.getPort();
        } catch (Exception e) {
            return -1;
        }
    }

    private static String schemeFromBrokerUrl(String url) {
        try {
            if (url == null || url.isBlank()) {
                return null;
            }
            return URI.create(firstUrlPart(url)).getScheme();
        } catch (Exception e) {
            return null;
        }
    }

    private static String firstUrlPart(String value) {
        int comma = value.indexOf(',');
        return comma >= 0 ? value.substring(0, comma) : value;
    }

    private static String hostFromBootstrap(String bootstrap) {
        HostPort hp = firstBootstrapHostPort(bootstrap);
        return hp == null ? null : hp.host();
    }

    private static int portFromBootstrap(String bootstrap) {
        HostPort hp = firstBootstrapHostPort(bootstrap);
        return hp == null ? -1 : hp.port();
    }

    private static HostPort firstBootstrapHostPort(String bootstrap) {
        if (bootstrap == null || bootstrap.isBlank()) {
            return null;
        }
        String first = bootstrap.split(",", 2)[0].trim();
        if (first.isBlank()) {
            return null;
        }
        if (first.startsWith("[")) {
            int end = first.indexOf(']');
            if (end > 0) {
                String host = first.substring(1, end);
                int port = -1;
                if (first.length() > end + 2 && first.charAt(end + 1) == ':') {
                    port = parseInt(first.substring(end + 2));
                }
                return new HostPort(host, port);
            }
        }
        int colon = first.lastIndexOf(':');
        if (colon > 0) {
            return new HostPort(first.substring(0, colon), parseInt(first.substring(colon + 1)));
        }
        return new HostPort(first, -1);
    }

    private static String resolveIp(String host) {
        try {
            return InetAddress.getByName(host).getHostAddress();
        } catch (Exception e) {
            return host;
        }
    }

    private static int parseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return -1;
        }
    }

    private static Integer integer(Object value) {
        if (value instanceof Number n) {
            return n.intValue();
        }
        if (value == null) {
            return null;
        }
        return parseInt(String.valueOf(value));
    }

    private static int firstPositive(Integer... values) {
        if (values == null) {
            return -1;
        }
        for (Integer value : values) {
            if (value != null && value > 0) {
                return value;
            }
        }
        return -1;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank() && !"<empty>".equals(value)) {
                return value;
            }
        }
        return null;
    }

    private static String nvl(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String string(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static String escapeHeader(String value) {
        return nvl(value, "")
                .replace("\\", "\\\\")
                .replace("|", "\\|")
                .replace("\n", " ")
                .replace("\r", " ");
    }

    private static String escapeExtension(String value) {
        return nvl(value, "")
                .replace("\\", "\\\\")
                .replace("=", "\\=")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    private record Endpoint(String hostIp, String hostName, String mac, int port) { }
    private record HostPort(String host, int port) { }
}
