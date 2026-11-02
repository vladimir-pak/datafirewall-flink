package com.gpb.datafirewall.transaction;

import static org.junit.jupiter.api.Assertions.*;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.JMSException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(10)
class BrokerWorkerTest {
    private static final MessageRecord MESSAGE = MessageRecord.fromJms("ID:test", "{}", null, 1L);
    private static TransactionalReply reply() {
        ProcessingResult result = ProcessingResult.forJms("ID:test", "{}", "{}", "{}");
        return new TransactionalReply(result, result);
    }

    @Test void idleReceiveDoesNotGeneratePollsAndOneThreadOwnsTheWholeTransaction() throws Exception {
        BlockingQueue<MessageRecord> input = new LinkedBlockingQueue<>();
        CountDownLatch waiting = new CountDownLatch(1), audited = new CountDownLatch(1);
        AtomicInteger polls = new AtomicInteger(), connects = new AtomicInteger();
        List<String> events = new CopyOnWriteArrayList<>();
        List<Thread> threads = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> fatal = new AtomicReference<>();
        TransactionBroker broker = new TransactionBroker() {
            public TransactionalReply poll(Processor processor) throws Exception {
                polls.incrementAndGet(); waiting.countDown();
                MessageRecord msg = input.take();
                threads.add(Thread.currentThread()); events.add("get");
                TransactionalReply result = processor.process(msg);
                events.add("reply"); events.add("commit");
                return result;
            }
            public void close() { threads.add(Thread.currentThread()); events.add("close"); }
        };
        BrokerWorker worker = new BrokerWorker("test-owner", () -> {
            connects.incrementAndGet(); threads.add(Thread.currentThread()); return broker;
        }, msg -> { threads.add(Thread.currentThread()); events.add("process"); return reply(); },
                audit -> { events.add("audit"); audited.countDown(); }, fatal::set);
        try {
            worker.start(); assertTrue(waiting.await(2, TimeUnit.SECONDS));
            assertFalse(audited.await(150, TimeUnit.MILLISECONDS));
            assertEquals(1, polls.get()); assertEquals(1, connects.get());
            input.add(MESSAGE); assertTrue(audited.await(2, TimeUnit.SECONDS));
            assertEquals(List.of("get", "process", "reply", "commit", "audit"), events);
            assertEquals(1, worker.repliesCommitted());
        } finally { worker.close(); assertTrue(worker.awaitTermination(2000)); }
        assertNull(fatal.get()); assertFalse(worker.isConnected());
        assertTrue(threads.stream().allMatch(t -> t == threads.get(0)));
        assertNotEquals(Thread.currentThread(), threads.get(0));
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void reconnectsAfterBrokerIoFailureWithoutFailingTask(boolean mq) throws Exception {
        AtomicInteger attempts = new AtomicInteger(); CountDownLatch recovered = new CountDownLatch(1);
        AtomicReference<Throwable> fatal = new AtomicReference<>();
        BrokerWorker worker = new BrokerWorker("retry", () -> {
            if (attempts.incrementAndGet() == 1) {
                if (mq) throw new MQException(MQConstants.MQCC_FAILED, MQConstants.MQRC_CONNECTION_BROKEN, null);
                throw new JMSException("connection lost");
            }
            return new TransactionBroker() {
                public TransactionalReply poll(Processor p) throws Exception {
                    recovered.countDown(); new CountDownLatch(1).await(); return null;
                }
                public void close() { }
            };
        }, msg -> reply(), ignored -> fail("No input"), fatal::set, 10, 20);
        try { worker.start(); assertTrue(recovered.await(2, TimeUnit.SECONDS)); }
        finally { worker.close(); assertTrue(worker.awaitTermination(2000)); }
        assertEquals(2, attempts.get()); assertNull(fatal.get());
    }

    @Test void cancellationDuringProcessingIsNotReturnedAsSuccessfulReply() throws Exception {
        CountDownLatch started = new CountDownLatch(1), rolledBack = new CountDownLatch(1);
        AtomicInteger commits = new AtomicInteger(); AtomicReference<Throwable> fatal = new AtomicReference<>();
        BrokerWorker worker = new BrokerWorker("cancel", () -> new TransactionBroker() {
            public TransactionalReply poll(Processor processor) throws Exception {
                try {
                    TransactionalReply r = processor.process(MESSAGE); commits.incrementAndGet(); return r;
                } catch (CancellationException cancelled) { rolledBack.countDown(); throw cancelled; }
            }
            public void close() { }
        }, msg -> {
            started.countDown();
            try { new CountDownLatch(1).await(); } catch (InterruptedException ignored) { }
            return reply();
        }, ignored -> fail("Cancelled transaction has no audit"), fatal::set);
        worker.start(); assertTrue(started.await(2, TimeUnit.SECONDS)); worker.close();
        assertTrue(worker.awaitTermination(2000)); assertEquals(0, commits.get());
        assertEquals(0, rolledBack.getCount()); assertNull(fatal.get());
    }

    @Test void unexpectedFailureIsReportedAndConnectionClosed() throws Exception {
        CountDownLatch reported = new CountDownLatch(1), closed = new CountDownLatch(1);
        IllegalStateException failure = new IllegalStateException("Unexpected failure");
        AtomicReference<Throwable> fatal = new AtomicReference<>();
        BrokerWorker worker = new BrokerWorker("fatal", () -> new TransactionBroker() {
            public TransactionalReply poll(Processor p) { throw failure; }
            public void close() { closed.countDown(); }
        }, msg -> reply(), ignored -> {}, error -> { fatal.set(error); reported.countDown(); });
        try { worker.start(); assertTrue(reported.await(2, TimeUnit.SECONDS)); }
        finally { worker.close(); assertTrue(worker.awaitTermination(2000)); }
        assertSame(failure, fatal.get()); assertEquals(0, closed.getCount());
    }
}
