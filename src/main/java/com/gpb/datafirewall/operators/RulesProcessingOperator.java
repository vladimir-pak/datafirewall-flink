package com.gpb.datafirewall.operators;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.bundle.CacheBundleEnvelope;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.services.MessageRecordCodec;
import com.gpb.datafirewall.services.RulesProcessingFunction;
import com.gpb.datafirewall.services.RuntimeEpoch;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.ThrowingRunnable;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * One background preparation per subtask; one complete runtime is published on its mailbox.
 * Managed state and output are never accessed by the executor. Updating an existing cache
 * never closes processing or waits for a checkpoint/another subtask.
 */
public final class RulesProcessingOperator
        extends AbstractUdfStreamOperator<ProcessingResult, RulesProcessingFunction>
        implements TwoInputStreamOperator<MessageRecord, CacheBundleEnvelope, ProcessingResult> {
    private final MailboxExecutor mailbox;
    private final int maxPending;
    private final long maxPendingBytes;
    private final Function<CacheBundleState, RuntimeEpoch> testPreparer;
    private final Deque<byte[]> pending = new ArrayDeque<>();
    private transient ListState<byte[]> pendingState;
    private transient ListState<byte[]> activeBundleState;
    private transient ListState<byte[]> requestedBundleState;
    private transient ExecutorService executor;
    private Image activeImage;
    private volatile Image requested;
    private volatile Image building;
    private long pendingBytes;
    private boolean drainScheduled;
    private volatile boolean closed;

    /** Byte arrays are private immutable checkpoint images, never modified after construction. */
    private record Image(long epoch, String checksum, byte[] bytes) {
        static Image restore(byte[] bytes) {
            CacheBundleState b = CacheBundleStateSerializer.decode(bytes);
            return new Image(b.cacheEpoch(), b.checksum(), bytes);
        }
    }

    public RulesProcessingOperator(RulesProcessingFunction function,
                                   MailboxExecutor mailbox, int maxPending, long maxPendingBytes) {
        this(function, mailbox, maxPending, maxPendingBytes, null);
    }
    /** Package-private injection for deterministic slow/failing preparation tests. */
    RulesProcessingOperator(RulesProcessingFunction function,
                            MailboxExecutor mailbox, int maxPending, long maxPendingBytes,
                            Function<CacheBundleState, RuntimeEpoch> testPreparer) {
        super(function);
        this.mailbox = mailbox;
        this.maxPending = maxPending; this.maxPendingBytes = maxPendingBytes;
        this.testPreparer = testPreparer;
    }

    @Override public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        pendingState = context.getOperatorStateStore().getListState(descriptor("cache-startup-pending-v1"));
        activeBundleState = context.getOperatorStateStore().getListState(descriptor("cache-local-active-v1"));
        requestedBundleState = context.getOperatorStateStore().getListState(descriptor("cache-local-requested-v1"));
        if (context.isRestored()) {
            for (byte[] message : pendingState.get()) addPending(message);
            // Multiple entries can be assigned here during rescaling. Full snapshots permit
            // choosing the newest local epoch; records are not pinned to historical epochs.
            for (byte[] bytes : activeBundleState.get()) activeImage = newest(activeImage, Image.restore(bytes));
            requested = activeImage;
            for (byte[] bytes : requestedBundleState.get()) requested = newest(requested, Image.restore(bytes));
        }
        var legacy = context.getOperatorStateStore().getBroadcastState(new org.apache.flink.api.common.state.MapStateDescriptor<>(
                "cache-bundle-broadcast-v1", org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                new CacheBundleStateSerializer()));
        if (activeImage == null) {
            CacheBundleState saved = legacy.get("active");
            if (saved != null) {
                activeImage = new Image(saved.cacheEpoch(), saved.checksum(), CacheBundleStateSerializer.encode(saved));
                requested = newest(requested, activeImage);
            }
        }
        legacy.clear();
    }
    private static ListStateDescriptor<byte[]> descriptor(String name) {
        return new ListStateDescriptor<>(name, BytePrimitiveArraySerializer.INSTANCE);
    }
    @Override public void open() throws Exception {
        super.open();
        executor = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "cache-revolver-" + getRuntimeContext().getIndexOfThisSubtask()
                    + "-" + getRuntimeContext().getAttemptNumber());
            thread.setDaemon(true);
            return thread;
        });
        getRuntimeContext().getMetricGroup().gauge("cacheActiveEpoch", () -> userFunction.activeEpoch());
        getRuntimeContext().getMetricGroup().gauge("cacheRequestedEpoch", () -> requested == null ? -1L : requested.epoch);
        getRuntimeContext().getMetricGroup().gauge("cachePreparationInProgress", () -> building == null ? 0 : 1);
        getRuntimeContext().getMetricGroup().gauge("cachePendingMessages", pending::size);
        getRuntimeContext().getMetricGroup().gauge("cacheProcessingEnabled", () -> ready() ? 1 : 0);
        // Restore the active version first, then prepare any saved pending update in the background.
        if (activeImage != null) startPreparation(activeImage); else startNextPreparation();
    }
    private boolean ready() { return userFunction.activeEpoch() >= 0; }
    @Override public void processElement1(StreamRecord<MessageRecord> record) throws Exception {
        if (!ready() || !pending.isEmpty()) {
            addPending(MessageRecordCodec.encode(record.getValue()));
            scheduleDrain();
        } else process(record.getValue());
    }
    @Override public void processElement2(StreamRecord<CacheBundleEnvelope> record) {
        CacheBundleEnvelope update = record.getValue();
        update.validateHeader();
        // Header checks are cheap. Full checksum validation and class loading happen in the executor.
        Image image = new Image(update.epoch, update.checksum, update.bytes);
        newest(activeImage, image);
        Image selected = newest(requested, image);
        if (selected == image) {
            // Flink object reuse must not mutate bytes retained for preparation/checkpointing.
            requested = new Image(image.epoch, image.checksum, image.bytes.clone());
            startNextPreparation();
        }
    }
    private static Image newest(Image a, Image b) {
        if (a == null) return b;
        if (b == null) return a;
        if (a.epoch == b.epoch && !Objects.equals(a.checksum, b.checksum))
            throw new IllegalStateException("Different contents for epoch " + b.epoch);
        return b.epoch > a.epoch ? b : a;
    }
    private void startNextPreparation() {
        if (!closed && building == null && requested != null
                && (!ready() || requested.epoch > userFunction.activeEpoch())) startPreparation(requested);
    }
    private void startPreparation(Image image) {
        building = image;
        executor.execute(() -> {
            try {
                CacheBundleState bundle = CacheBundleStateSerializer.decode(image.bytes);
                if (bundle.cacheEpoch() != image.epoch || !bundle.checksum().equals(image.checksum))
                    throw new IllegalStateException("Cache transfer identity mismatch");
                RuntimeEpoch prepared = testPreparer == null
                        ? userFunction.prepareEpoch(bundle) : testPreparer.apply(bundle);
                post(() -> completePreparation(image, prepared, null));
            } catch (Throwable failure) { post(() -> completePreparation(image, null, failure)); }
        });
    }
    private void completePreparation(Image image, RuntimeEpoch prepared, Throwable failure) {
        if (closed || building != image) return;
        building = null;
        boolean obsolete = requested != null && image.epoch < requested.epoch;
        if (failure != null) {
            // Superseded work is no longer needed; a failure of the desired epoch fails the task.
            if (!obsolete) throw new IllegalStateException("Cache preparation failed epoch=" + image.epoch, failure);
        } else if (!ready() || !obsolete) {
            // Both state pointers and the runtime publication change in one mailbox action.
            // A checkpoint/message cannot observe a half-applied update.
            if (prepared.cacheEpoch() != image.epoch) throw new IllegalStateException("Prepared epoch mismatch");
            userFunction.activate(prepared);
            activeImage = image;
            scheduleDrain();
        }
        startNextPreparation();
    }
    private void post(ThrowingRunnable<? extends Exception> action) {
        if (closed) return;
        try {
            mailbox.execute(() -> { if (!closed) action.run(); }, "Publish prepared cache epoch");
        } catch (RejectedExecutionException rejected) {
            if (!closed) getContainingTask().handleAsyncException("Cache completion mailbox rejected", rejected);
        }
    }
    private void addPending(byte[] bytes) {
        if (pending.size() >= maxPending || bytes.length > maxPendingBytes - pendingBytes)
            throw new IllegalStateException("Cache startup buffer limit exceeded; messages=" + pending.size()
                    + " bytes=" + pendingBytes + ". Size cache.startup.max-pending-* for startup/recovery.");
        pending.addLast(bytes); pendingBytes += bytes.length;
    }
    private void scheduleDrain() {
        if (closed || !ready() || pending.isEmpty() || drainScheduled) return;
        drainScheduled = true;
        mailbox.execute(() -> {
            drainScheduled = false;
            if (closed) return;
            for (int i = 0; i < 64 && !pending.isEmpty(); i++) {
                byte[] bytes = pending.peekFirst();
                process(MessageRecordCodec.decode(bytes));
                pending.removeFirst(); pendingBytes -= bytes.length;
            }
            scheduleDrain();
        }, "Drain messages after initial cache preparation");
    }
    private void process(MessageRecord record) {
        userFunction.process(record,
                result -> output.collect(new StreamRecord<>(result)),
                result -> output.collect(RulesProcessingFunction.FLINK_AUDIT_RESULT_TAG, new StreamRecord<>(result)));
    }
    @Override public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        pendingState.update(new ArrayList<>(pending));
        activeBundleState.update(activeImage == null ? List.of() : List.of(activeImage.bytes));
        requestedBundleState.update(requested == null || (activeImage != null && requested.epoch <= activeImage.epoch)
                ? List.of() : List.of(requested.bytes));
    }
    private void stopPreparation() {
        closed = true;
        if (executor != null) executor.shutdownNow(); // never wait for a rule constructor on the mailbox
    }
    @Override public void close() throws Exception { stopPreparation(); super.close(); }
}
