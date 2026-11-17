package com.gpb.datafirewall.transaction.mq;

import com.gpb.datafirewall.util.SslStoreTypes;
import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Hashtable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MqConnect {
    private static final Logger log = LoggerFactory.getLogger(MqConnect.class);

    private MqConnect() { }

    /** Existing callers retain JKS as the application default. */
    public static MQQueueManager connect(String qmgr, String host, int port, String channel,
            String user, String password, boolean tlsEnabled, String cipherSuite,
            String trustStore, String trustStorePassword) throws MQException {
        return connect(qmgr, host, port, channel, user, password, tlsEnabled, cipherSuite,
                trustStore, trustStorePassword, "JKS");
    }

    public static MQQueueManager connect(String qmgr, String host, int port, String channel,
            String user, String password, boolean tlsEnabled, String cipherSuite,
            String trustStore, String trustStorePassword, String trustStoreType) throws MQException {
        if (qmgr == null || qmgr.isBlank()) {
            throw new IllegalArgumentException("IBM MQ queue manager name must be provided");
        }
        Hashtable<String, Object> props = connectionProperties(host, port, channel, user,
                password, tlsEnabled, cipherSuite, trustStore, trustStorePassword, trustStoreType);
        boolean authEnabled = user != null && !user.isBlank();
        log.info("Connecting to MQ qmgr={} {}:{} channel={} auth={} tls={} cipherSuite={}",
                qmgr, host, port, channel, authEnabled, tlsEnabled, cipherSuite);
        return new MQQueueManager(qmgr, props);
    }

    static Hashtable<String, Object> connectionProperties(String host, int port, String channel,
            String user, String password, boolean tlsEnabled, String cipherSuite,
            String trustStore, String trustStorePassword, String trustStoreType) {
        Hashtable<String, Object> props = new Hashtable<>();
        props.put(MQConstants.HOST_NAME_PROPERTY, host);
        props.put(MQConstants.PORT_PROPERTY, port);
        props.put(MQConstants.CHANNEL_PROPERTY, channel);
        props.put(MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_CLIENT);
        if (user != null && !user.isBlank()) {
            props.put(MQConstants.USER_ID_PROPERTY, user);
            props.put(MQConstants.PASSWORD_PROPERTY, password);
            props.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, true);
        }
        if (tlsEnabled) {
            if (cipherSuite != null && !cipherSuite.isBlank()) {
                props.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, cipherSuite);
            }
            if (trustStore != null && !trustStore.isBlank()) {
                // Per connection: different jobs in one TaskManager may use different stores.
                props.put(MQConstants.SSL_SOCKET_FACTORY_PROPERTY,
                        sslSocketFactory(trustStore, trustStorePassword, trustStoreType));
            }
        }
        return props;
    }

    static SSLSocketFactory sslSocketFactory(String path, String password, String configuredType) {
        String type = SslStoreTypes.normalize(configuredType);
        char[] chars = password == null ? null : password.toCharArray();
        try {
            KeyStore store = KeyStore.getInstance(type);
            try (InputStream in = Files.newInputStream(Path.of(path))) {
                store.load(in, chars);
            }
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            // Keep support for a client certificate supplied through existing JVM settings.
            context.init(systemKeyManagers(), trust.getTrustManagers(), null);
            return context.getSocketFactory();
        } catch (Exception failure) {
            throw new IllegalStateException("Cannot initialize IBM MQ TLS truststore: " + path
                    + ", type=" + type, failure);
        } finally {
            if (chars != null) Arrays.fill(chars, '\0');
        }
    }

    private static KeyManager[] systemKeyManagers() throws Exception {
        String path = System.getProperty("javax.net.ssl.keyStore");
        if (path == null || path.isBlank()) return null;
        String type = System.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
        String provider = System.getProperty("javax.net.ssl.keyStoreProvider");
        char[] password = System.getProperty("javax.net.ssl.keyStorePassword", "").toCharArray();
        try {
            KeyStore store = provider == null || provider.isBlank()
                    ? KeyStore.getInstance(type) : KeyStore.getInstance(type, provider);
            if ("NONE".equals(path)) {
                store.load(null, password);
            } else {
                try (InputStream in = Files.newInputStream(Path.of(path))) {
                    store.load(in, password);
                }
            }
            KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, password);
            return keys.getKeyManagers();
        } finally {
            Arrays.fill(password, '\0');
        }
    }
}
