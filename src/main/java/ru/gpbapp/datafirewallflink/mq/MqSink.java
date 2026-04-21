package ru.gpbapp.datafirewallflink.mq;

import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class MqSink extends RichSinkFunction<MessageReply> {

    private static final Logger log = LoggerFactory.getLogger(MqSink.class);

    private final String host;
    private final int port;
    private final String channel;
    private final String qmgr;
    private final String outQueue;
    private final String user;
    private final String password;

    private transient MQQueueManager qm;
    private transient MQQueue queue;

    public MqSink(
            String host,
            int port,
            String channel,
            String qmgr,
            String outQueue,
            String user,
            String password
    ) {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.qmgr = qmgr;
        this.outQueue = outQueue;
        this.user = user;
        this.password = password;
    }

    public MqSink(String host, int port, String channel, String qmgr, String outQueue) {
        this(host, port, channel, qmgr, outQueue, null, null);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        try {
            log.info(
                    "MqSink connecting: subtask={} host={} port={} qmgr={} channel={} queue={} user={}",
                    subtask,
                    host,
                    port,
                    qmgr,
                    channel,
                    outQueue,
                    user
            );

            qm = MqConnect.connect(qmgr, host, port, channel, user, password);

            int openOptions = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING;
            queue = qm.accessQueue(outQueue, openOptions);

            log.info("MqSink opened queue={} subtask={}", outQueue, subtask);
        } catch (Exception e) {
            close();
            throw new RuntimeException(
                    "Failed to open MqSink. host=" + host +
                            ", port=" + port +
                            ", qmgr=" + qmgr +
                            ", channel=" + channel +
                            ", queue=" + outQueue,
                    e
            );
        }
    }

    @Override
    public void invoke(MessageReply value, Context context) throws Exception {
        if (value == null) {
            return;
        }

        String payload = value.payload;
        byte[] body = payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8);

        MQMessage msg = new MQMessage();
        msg.format = MQConstants.MQFMT_STRING;
        msg.characterSet = 1208;

        if (value.correlId != null) {
            msg.correlationId = value.correlId;
        }

        if (body.length > 0) {
            msg.write(body);
        }

        MQPutMessageOptions pmo = new MQPutMessageOptions();
        pmo.options = MQConstants.MQPMO_NO_SYNCPOINT | MQConstants.MQPMO_FAIL_IF_QUIESCING;

        queue.put(msg, pmo);
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
            if (qm != null) {
                qm.disconnect();
            }
        } catch (Exception e) {
            log.warn("Failed to disconnect MQQueueManager", e);
        } finally {
            qm = null;
        }
    }
}