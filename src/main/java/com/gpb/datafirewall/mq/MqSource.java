package com.gpb.datafirewall.mq;

import com.gpb.datafirewall.services.MessageRecord;
import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

public class MqSource extends RichSourceFunction<MessageRecord> {

    private static final Logger log = LoggerFactory.getLogger(MqSource.class);

    private static final Map<Integer, Charset> CCSID_MAP = Map.ofEntries(
            Map.entry(1208, StandardCharsets.UTF_8),
            Map.entry(1200, StandardCharsets.UTF_16),
            Map.entry(819, StandardCharsets.ISO_8859_1),
            Map.entry(1251, Charset.forName("windows-1251")),
            Map.entry(1252, Charset.forName("windows-1252")),
            Map.entry(866, Charset.forName("Cp866")),
            Map.entry(850, Charset.forName("Cp850"))
    );

    private final boolean logPayloads;
    private final int logPreviewLen;

    private final String host;
    private final int port;
    private final String channel;
    private final String qmgr;
    private final String queueName;
    private final String user;
    private final String password;
    private final int waitIntervalMs;

    private final boolean tlsEnabled;
    private final String tlsCipherSuite;
    private final String trustStore;
    private final String trustStorePassword;

    private transient volatile boolean running;
    private transient MQQueueManager qMgr;
    private transient MQQueue queue;

    public MqSource(
            String host,
            int port,
            String channel,
            String qmgr,
            String queueName,
            String user,
            String password
    ) {
        this(
                host,
                port,
                channel,
                qmgr,
                queueName,
                user,
                password,
                false,
                600,
                1000,
                false,
                null,
                null,
                null
        );
    }

    public MqSource(
            String host,
            int port,
            String channel,
            String qmgr,
            String queueName,
            String user,
            String password,
            boolean logPayloads,
            int logPreviewLen,
            int waitIntervalMs,
            boolean tlsEnabled,
            String tlsCipherSuite,
            String trustStore,
            String trustStorePassword
    ) {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.qmgr = qmgr;
        this.queueName = queueName;
        this.user = user;
        this.password = password;
        this.logPayloads = logPayloads;
        this.logPreviewLen = logPreviewLen;
        this.waitIntervalMs = waitIntervalMs;
        this.tlsEnabled = tlsEnabled;
        this.tlsCipherSuite = tlsCipherSuite;
        this.trustStore = trustStore;
        this.trustStorePassword = trustStorePassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        running = true;

        int openOptions = MQConstants.MQOO_INPUT_SHARED | MQConstants.MQOO_FAIL_IF_QUIESCING;

        try {
            log.info(
                    "MqSource connecting: subtask={} host={} port={} qmgr={} channel={} queue={} user={} tls={} cipherSuite={} trustStore={}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    host,
                    port,
                    qmgr,
                    channel,
                    queueName,
                    user,
                    tlsEnabled,
                    tlsCipherSuite == null || tlsCipherSuite.isBlank() ? "<empty>" : tlsCipherSuite,
                    trustStore == null || trustStore.isBlank() ? "<empty>" : trustStore
            );

            qMgr = MqConnect.connect(
                    qmgr,
                    host,
                    port,
                    channel,
                    user,
                    password,
                    tlsEnabled,
                    tlsCipherSuite,
                    trustStore,
                    trustStorePassword
            );

            queue = qMgr.accessQueue(queueName, openOptions);

            log.info(
                    "MqSource opened queue={} subtask={} log.payloads={} log.preview.len={} wait.ms={}",
                    queueName,
                    getRuntimeContext().getIndexOfThisSubtask(),
                    logPayloads,
                    logPreviewLen,
                    waitIntervalMs
            );
        } catch (Exception e) {
            close();
            throw new RuntimeException(
                    "Failed to open MqSource. host=" + host +
                            ", port=" + port +
                            ", qmgr=" + qmgr +
                            ", channel=" + channel +
                            ", queue=" + queueName +
                            ", tlsEnabled=" + tlsEnabled,
                    e
            );
        }
    }

    @Override
    public void run(SourceContext<MessageRecord> ctx) throws Exception {
        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
        gmo.waitInterval = waitIntervalMs;

        while (running) {
            MQMessage msg = new MQMessage();

            try {
                queue.get(msg, gmo);

                int messageLength = msg.getMessageLength();
                int dataLength = msg.getDataLength();

                Long createdDttm = extractMqPutTimestamp(msg);
                Long readedDttm = currentTimestampMs();

                log.debug(
                        "MQ message props: ccsid={} encoding={} format={} dataLength={} msgLength={}",
                        msg.characterSet,
                        msg.encoding,
                        msg.format,
                        dataLength,
                        messageLength
                );

                byte[] buf = new byte[dataLength];
                msg.readFully(buf);

                Charset cs = detectCharset(msg);
                String body = new String(buf, cs);

                byte[] msgIdBytes = Arrays.copyOf(msg.messageId, msg.messageId.length);
                String msgIdHex = toHexSafe(msgIdBytes);

                boolean endsWithJsonClose = body != null && body.trim().endsWith("}");

                log.info(
                        "MQ READ subtask={} msgIdLen={} messageLength={} dataLength={} bytesRead={} bodyChars={} endsWithJsonClose={}",
                        getRuntimeContext().getIndexOfThisSubtask(),
                        msgIdBytes.length,
                        messageLength,
                        dataLength,
                        buf.length,
                        body.length(),
                        endsWithJsonClose
                );

                if (logPayloads) {
                    log.info("MQ READ msgId={} BODY:\n{}", msgIdHex, body);
                } else {
                    log.info("MQ READ msgId={} BODY preview={}", msgIdHex, preview(body, logPreviewLen));
                }

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(MessageRecord.fromMq(msgIdBytes, body, createdDttm, readedDttm));
                }

            } catch (MQException mqe) {
                if (!running) {
                    log.info("MqSource stopped during shutdown.");
                    return;
                }

                if (mqe.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                    continue;
                }

                log.error(
                        "MQ GET failed (completionCode={}, reasonCode={})",
                        mqe.completionCode,
                        mqe.reasonCode,
                        mqe
                );

                throw mqe;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        close();
    }

    @Override
    public void close() {
        try {
            if (queue != null) {
                queue.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close MQQueue", e);
        } finally {
            queue = null;
        }

        try {
            if (qMgr != null) {
                qMgr.disconnect();
            }
        } catch (Exception e) {
            log.warn("Failed to disconnect MQQueueManager", e);
        } finally {
            qMgr = null;
        }
    }

    private Charset detectCharset(MQMessage msg) {
        int ccsid = msg.characterSet;

        if (ccsid <= 0) {
            return StandardCharsets.UTF_8;
        }

        Charset mapped = CCSID_MAP.get(ccsid);
        if (mapped != null) {
            return mapped;
        }

        Charset cs = tryCharset("Cp" + ccsid);
        if (cs != null) {
            return cs;
        }

        cs = tryCharset("IBM" + ccsid);
        if (cs != null) {
            return cs;
        }

        cs = tryCharset("x-IBM" + ccsid);
        if (cs != null) {
            return cs;
        }

        log.debug("Unknown CCSID={} -> fallback UTF-8", ccsid);
        return StandardCharsets.UTF_8;
    }

    private Charset tryCharset(String name) {
        if (name == null) {
            return null;
        }

        String n = name.trim();
        if (n.isEmpty()) {
            return null;
        }

        boolean hasDigit = false;
        for (int i = 0; i < n.length(); i++) {
            if (Character.isDigit(n.charAt(i))) {
                hasDigit = true;
                break;
            }
        }

        if (!hasDigit) {
            log.debug("Skip charset candidate (no digits): '{}'", n);
            return null;
        }

        try {
            return Charset.forName(n);
        } catch (Exception e) {
            log.debug("Charset not supported: '{}'", n);
            return null;
        }
    }

    private static String preview(String s, int max) {
        if (s == null) {
            return "null";
        }
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max) + "...(+" + (s.length() - max) + " chars)";
    }

    private static String toHexSafe(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }

        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02X", b));
        }
        return sb.toString();
    }

    private Long extractMqPutTimestamp(MQMessage msg) {
        if (msg == null || msg.putDateTime == null) {
            return null;
        }

        try {
            return msg.putDateTime.getTimeInMillis();
        } catch (Exception e) {
            log.warn("Failed to extract IBM MQ putDateTime", e);
            return null;
        }
    }

    private Long currentTimestampMs() {
        return Instant.now().toEpochMilli();
    }
}