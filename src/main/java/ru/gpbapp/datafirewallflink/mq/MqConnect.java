package ru.gpbapp.datafirewallflink.mq;

import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;

public final class MqConnect {

    private static final Logger log = LoggerFactory.getLogger(MqConnect.class);

    private MqConnect() {
    }

    public static MQQueueManager connect(
            String qmgr,
            String host,
            int port,
            String channel,
            String user,
            String password,
            boolean tlsEnabled,
            String cipherSuite,
            String trustStore,
            String trustStorePassword
    ) throws Exception {

        if (tlsEnabled) {
            setSystemPropertyIfPresent("javax.net.ssl.trustStore", trustStore);
            setSystemPropertyIfPresent("javax.net.ssl.trustStorePassword", trustStorePassword);
        }

        Hashtable<String, Object> props = new Hashtable<>();

        props.put(MQConstants.HOST_NAME_PROPERTY, host);
        props.put(MQConstants.PORT_PROPERTY, port);
        props.put(MQConstants.CHANNEL_PROPERTY, channel);
        props.put(MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_CLIENT);

        boolean authEnabled = user != null && !user.isBlank();

        if (authEnabled) {
            props.put(MQConstants.USER_ID_PROPERTY, user);
            props.put(MQConstants.PASSWORD_PROPERTY, password);
            props.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, true);
        }

        if (tlsEnabled && cipherSuite != null && !cipherSuite.isBlank()) {
            props.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, cipherSuite);
        }

        log.info(
                "Connecting to MQ qmgr={} {}:{} channel={} auth={} tls={} cipherSuite={}",
                qmgr, host, port, channel, authEnabled, tlsEnabled, cipherSuite
        );

        return new MQQueueManager(qmgr, props);
    }

    private static void setSystemPropertyIfPresent(String key, String value) {
        if (value != null && !value.isBlank()) {
            System.setProperty(key, value);
        }
    }
}