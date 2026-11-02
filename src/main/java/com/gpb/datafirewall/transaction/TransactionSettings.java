package com.gpb.datafirewall.transaction;

import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.transaction.artemis.ArtemisTransactionalBroker;
import com.gpb.datafirewall.transaction.mq.MqTransactionalBroker;

import java.io.Serializable;

/** Serializable connection settings; client connections are created only inside the subtask. */
public record TransactionSettings(
        String backend,
        String host, int port, String channel, String queueManager,
        String inputQueue, String outputQueue,
        String username, String password,
        boolean tls, String tlsCipherSuite, String trustStore, String trustStorePassword,
        String xFrom, String xServiceId,
        String artemisUrl,
        long receiveTimeoutMs) implements Serializable {

    public TransactionSettings {
        if (!"mq".equals(backend) && !"artemis".equals(backend)) {
            throw new IllegalArgumentException("Unknown broker: " + backend);
        }
        if (inputQueue == null || inputQueue.isBlank() || outputQueue == null || outputQueue.isBlank()) {
            throw new IllegalArgumentException("input/output queue must be configured");
        }
        if (inputQueue.equals(outputQueue)) {
            throw new IllegalArgumentException("Broker input and output queues must be distinct");
        }
        if (receiveTimeoutMs < 1 || receiveTimeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid receive timeout: " + receiveTimeoutMs);
        }
    }

    public TransactionBroker connect() throws Exception {
        return connect(null, -1, -1);
    }

    public TransactionBroker connect(AuditConfig auditConfig, int subtaskIndex, int parallelism)
            throws Exception {
        if ("mq".equals(backend)) {
            return new MqTransactionalBroker(
                    host, port, channel, queueManager, inputQueue, outputQueue,
                    username, password, (int) receiveTimeoutMs,
                    tls, tlsCipherSuite, trustStore, trustStorePassword, xFrom, xServiceId,
                    auditConfig, subtaskIndex, parallelism);
        }
        return new ArtemisTransactionalBroker(
                artemisUrl, username, password, inputQueue, outputQueue,
                receiveTimeoutMs, auditConfig, subtaskIndex, parallelism);
    }
}
