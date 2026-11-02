package com.gpb.datafirewall.transaction;

import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.ibm.mq.MQException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns connection, blocking receive, processing, reply, commit and close on one thread. */
public final class BrokerWorker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerWorker.class);

    @FunctionalInterface
    public interface Connector {
        TransactionBroker connect() throws Exception;
    }

    private final Connector connector;
    private final TransactionBroker.Processor processor;
    private final Consumer<ProcessingResult> committedAudit;
    private final Consumer<Throwable> fatalError;
    private final BrokerReconnectBackoff backoff;
    private final Thread thread;
    private final AtomicLong repliesCommitted = new AtomicLong();
    private volatile boolean closed;
    private volatile boolean connected;
    private volatile boolean processing;

    public BrokerWorker(String name, Connector connector, TransactionBroker.Processor processor,
            Consumer<ProcessingResult> committedAudit, Consumer<Throwable> fatalError) {
        this(name, connector, processor, committedAudit, fatalError, 5_000L, 60_000L);
    }

    BrokerWorker(String name, Connector connector, TransactionBroker.Processor processor,
            Consumer<ProcessingResult> committedAudit, Consumer<Throwable> fatalError,
            long retryInitialMs, long retryMaxMs) {
        this.connector = connector;
        this.processor = processor;
        this.committedAudit = committedAudit;
        this.fatalError = fatalError;
        this.backoff = new BrokerReconnectBackoff(retryInitialMs, retryMaxMs);
        this.thread = new Thread(this::run, name);
        this.thread.setDaemon(true);
    }

    public void start() { thread.start(); }
    public boolean isConnected() { return connected; }
    public boolean isProcessing() { return processing; }
    public long repliesCommitted() { return repliesCommitted.get(); }

    private void run() {
        try {
            while (!closed) {
                try (TransactionBroker broker = connector.connect()) {
                    connected = true;
                    log.info("Broker connected: worker={}", thread.getName());
                    while (!closed) {
                        // poll blocks at the broker while idle; no timer/pulse/record is generated.
                        TransactionalReply result = broker.poll(this::processUnlessClosed);
                        backoff.onSuccess();
                        if (result != null) {
                            repliesCommitted.incrementAndGet();
                            if (!closed) committedAudit.accept(result.audit());
                        }
                    }
                } catch (MQException | JMSException | JMSRuntimeException unavailable) {
                    connected = false;
                    if (closed) break;
                    long delay = backoff.onFailure(System.nanoTime());
                    log.warn("Broker unavailable: worker={}, retry in {} ms: {}",
                            thread.getName(), delay, unavailable.toString());
                    // Only the dedicated receiver sleeps. Flink's mailbox stays available.
                    TimeUnit.MILLISECONDS.sleep(delay);
                } finally {
                    connected = false;
                }
            }
        } catch (Throwable failure) {
            if (!closed) fatalError.accept(failure);
        }
    }

    private TransactionalReply processUnlessClosed(MessageRecord message) throws Exception {
        checkCancellation();
        processing = true;
        try {
            TransactionalReply result = processor.process(message);
            checkCancellation();
            return result;
        } catch (Exception failure) {
            // A stop/interrupt is not a business error that may acknowledge the input.
            if (closed) {
                CancellationException cancelled = new CancellationException("Broker worker stopped");
                cancelled.initCause(failure);
                throw cancelled;
            }
            throw failure;
        } finally {
            processing = false;
        }
    }

    private void checkCancellation() {
        if (closed) throw new CancellationException("Broker worker stopped");
    }

    @Override public void close() {
        closed = true;
        thread.interrupt();
        // Never call MQ/JMS close concurrently with a receive/transaction. The owner closes
        // the connection in finally. A finite broker receive timeout bounds an idle wait.
    }

    public boolean awaitTermination(long timeoutMs) throws InterruptedException {
        if (Thread.currentThread() == thread) return false;
        thread.join(Math.max(1, timeoutMs));
        return !thread.isAlive();
    }
}
