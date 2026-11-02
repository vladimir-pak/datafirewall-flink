package com.gpb.datafirewall.transaction;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** One request-to-poll trigger per worker, NOT an acknowledgement or an MQ record. */
@SuppressWarnings("deprecation")
public final class TransactionPulseSource extends RichParallelSourceFunction<Long> {
    private final long intervalMs;
    private volatile boolean running = true;

    public TransactionPulseSource(long intervalMs) {
        if (intervalMs < 1) throw new IllegalArgumentException("intervalMs must be >= 1");
        this.intervalMs = intervalMs;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(1L);
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void cancel() { running = false; }
}
