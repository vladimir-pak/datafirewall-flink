package com.gpb.datafirewall.transaction.kafka;

import static org.junit.jupiter.api.Assertions.*;
import com.gpb.datafirewall.dto.ProcessingResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.jupiter.api.Test;

class MailboxAuditBufferTest {
    static final class Mailbox implements MailboxExecutor {
        final ConcurrentLinkedQueue<ThrowingRunnable<? extends Exception>> actions = new ConcurrentLinkedQueue<>();
        public void execute(MailOptions options, ThrowingRunnable<? extends Exception> action,
                String description, Object... args) { actions.add(action); }
        public void yield() { throw new UnsupportedOperationException(); }
        public boolean tryYield() { throw new UnsupportedOperationException(); }
        public boolean shouldInterrupt() { return false; }
        void drain() throws Exception { for (var action = actions.poll(); action != null; action = actions.poll()) action.run(); }
    }
    static ProcessingResult result(String id) { return ProcessingResult.forJms(id, "{}", "{}", "{}"); }

    @Test void blockedMailboxKeepsBoundedAuditAndOnlyOneScheduledAction() throws Exception {
        Mailbox mailbox = new Mailbox(); List<ProcessingResult> out = new ArrayList<>();
        MailboxAuditBuffer buffer = new MailboxAuditBuffer(mailbox, out::add, e -> fail(e), 2, 100_000);
        for (int i = 0; i < 1000; i++) buffer.offer(result("ID:" + i));
        assertEquals(2, buffer.size()); assertEquals(998, buffer.dropped());
        assertEquals(1, mailbox.actions.size()); assertTrue(out.isEmpty());
        mailbox.drain(); assertEquals(2, out.size()); assertEquals(0, buffer.size());
        buffer.offer(result("ID:next")); mailbox.drain(); assertEquals(3, out.size());
        assertEquals(0, buffer.retainedBytes());
        buffer.close();
    }

    @Test void oversizedAuditIsDroppedAndCloseFencesQueuedCallback() throws Exception {
        Mailbox mailbox = new Mailbox(); List<ProcessingResult> out = new ArrayList<>();
        MailboxAuditBuffer buffer = new MailboxAuditBuffer(mailbox, out::add, e -> fail(e), 10, 1024);
        ProcessingResult huge = result("ID:big"); huge.setOriginalJson("x".repeat(2048));
        buffer.offer(huge); assertEquals(1, buffer.dropped()); assertTrue(mailbox.actions.isEmpty());
        buffer.offer(result("ID:small")); assertEquals(1, buffer.size());
        buffer.close(); mailbox.drain(); assertTrue(out.isEmpty()); assertEquals(0, buffer.retainedBytes());
    }
}
