package com.gpb.datafirewall.transaction.kafka;

import com.gpb.datafirewall.dto.ProcessingResult;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import org.apache.flink.api.common.operators.MailboxExecutor;

/** Bounded, deliberately lossy handoff AFTER commit. Only the mailbox emits Flink records. */
final class MailboxAuditBuffer implements AutoCloseable {
    private record Entry(ProcessingResult result, long bytes) { }
    private final MailboxExecutor mailbox;
    private final Consumer<ProcessingResult> output;
    private final Consumer<Throwable> fatalError;
    private final int maxRecords;
    private final long maxBytes;
    private final Deque<Entry> entries = new ArrayDeque<>();
    private long bytes;
    private long dropped;
    private boolean scheduled;
    private boolean closed;

    MailboxAuditBuffer(MailboxExecutor mailbox, Consumer<ProcessingResult> output,
            Consumer<Throwable> fatalError, int maxRecords, long maxBytes) {
        if (maxRecords < 1 || maxBytes < 1) throw new IllegalArgumentException("Invalid audit buffer limits");
        this.mailbox = mailbox;
        this.output = output;
        this.fatalError = fatalError;
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
    }

    void offer(ProcessingResult result) {
        if (result == null) return;
        long size = estimateBytes(result);
        boolean submit;
        synchronized (this) {
            if (closed) return;
            if (entries.size() >= maxRecords || size > maxBytes - bytes) {
                dropped++;
                return;
            }
            entries.addLast(new Entry(result, size));
            bytes += size;
            submit = !scheduled;
            scheduled = true;
        }
        if (submit) submitDrain();
    }

    private void submitDrain() {
        try {
            mailbox.execute(this::drain, "Emit committed broker audit");
        } catch (RejectedExecutionException rejected) {
            boolean report;
            synchronized (this) { report = !closed; close(); }
            if (report) fatalError.accept(rejected);
        }
    }

    private void drain() {
        for (int i = 0; i < 64; i++) {
            Entry entry;
            synchronized (this) {
                if (closed) return;
                entry = entries.pollFirst();
                if (entry == null) break;
                bytes -= entry.bytes;
            }
            output.accept(entry.result);
        }
        boolean again;
        synchronized (this) {
            again = !closed && !entries.isEmpty();
            scheduled = again;
        }
        if (again) submitDrain();
    }

    synchronized int size() { return entries.size(); }
    synchronized long retainedBytes() { return bytes; }
    synchronized long dropped() { return dropped; }

    private static long stringBytes(String value) { return value == null ? 0 : 40L + 2L * value.length(); }

    private static long estimateBytes(ProcessingResult result) {
        long size = 256 + stringBytes(result.getOriginalJson()) + stringBytes(result.getShortJson())
                + stringBytes(result.getDetailJson()) + stringBytes(result.getEventId())
                + stringBytes(result.getJmsCorrelationId());
        Map<String, Object> headers = result.getHeaders();
        if (headers != null) for (var header : headers.entrySet()) {
            Object value = header.getValue();
            size += 64 + stringBytes(header.getKey())
                    + (value instanceof byte[] array ? array.length : stringBytes(String.valueOf(value)));
        }
        return size;
    }

    @Override public synchronized void close() {
        closed = true;
        entries.clear();
        bytes = 0;
        scheduled = false;
    }
}
