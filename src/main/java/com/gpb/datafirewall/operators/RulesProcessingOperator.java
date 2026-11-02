package com.gpb.datafirewall.operators;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.bundle.CacheBundleEnvelope;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.services.MessageRecordCodec;
import com.gpb.datafirewall.services.RulesProcessingFunction;
import com.gpb.datafirewall.services.PreparedRuntime;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
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
    private final Function<CacheBundleState, PreparedRuntime> testPreparer;
    private final Deque<byte[]> pending = new ArrayDeque<>();
    private transient ListState<byte[]> pendingState;
    private transient ListState<byte[]> activeBundleState;
    private transient ListState<byte[]> requestedBundleState;
    private transient ListState<String> selectedHandlerState;
    private String selectedHandler;
    private transient ExecutorService executor;
    private Image activeImage;
    private volatile Image requested;
    private volatile Image building;
    private long pendingBytes;
    private boolean drainScheduled;
    private volatile boolean closed;

    /** Immutable checkpoint images, identified by independent component versions. */
    private record Image(int rules, int politics, String checksum, byte[] bytes) {
        static Image restore(byte[] bytes) {
            CacheBundleState b = CacheBundleStateSerializer.decode(bytes);
            return new Image(Integer.parseInt(b.compiledRulesVersion()),
                    Integer.parseInt(b.politicsVersion()), b.checksum(), bytes);
        }
        boolean sameContent(Image other) {
            return other != null && Objects.equals(checksum, other.checksum);
        }
        String versions() { return "rules=" + rules + " politics=" + politics; }
    }

    public RulesProcessingOperator(RulesProcessingFunction function,
                                   MailboxExecutor mailbox, int maxPending, long maxPendingBytes) {
        this(function, mailbox, maxPending, maxPendingBytes, null);
    }
    /** Package-private injection for deterministic slow/failing preparation tests. */
    RulesProcessingOperator(RulesProcessingFunction function,
                            MailboxExecutor mailbox, int maxPending, long maxPendingBytes,
                            Function<CacheBundleState, PreparedRuntime> testPreparer) {
        super(function);
        this.mailbox = mailbox;
        this.maxPending = maxPending; this.maxPendingBytes = maxPendingBytes;
        this.testPreparer = testPreparer;
    }

    @Override public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        pendingState = context.getOperatorStateStore().getListState(descriptor("cache-startup-pending-v1"));
        activeBundleState = context.getOperatorStateStore().getListState(descriptor("cache-local-active-v2"));
        requestedBundleState = context.getOperatorStateStore().getListState(descriptor("cache-local-requested-v2"));
        selectedHandlerState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("cache-local-handler-v1", StringSerializer.INSTANCE));
        if (context.isRestored()) {
            for (byte[] message : pendingState.get()) addPending(message);
            // During rescaling reject incomparable states rather than silently rolling back a component.
            for (byte[] bytes : activeBundleState.get()) activeImage = newestRestored(activeImage, Image.restore(bytes));
            requested = activeImage;
            for (byte[] bytes : requestedBundleState.get()) requested = newestRestored(requested, Image.restore(bytes));
            for (String handler : selectedHandlerState.get()) {
                if (selectedHandler != null && !selectedHandler.equals(handler)) {
                    throw new IllegalStateException("Conflicting handlers in restored operator state");
                }
                selectedHandler = handler;
            }
        }
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
        getRuntimeContext().getMetricGroup().gauge("cacheActiveRulesVersion",
                () -> userFunction.cacheReady()
                        ? Long.parseLong(userFunction.currentRuntime().compiledRulesVersion()) : -1L);
        getRuntimeContext().getMetricGroup().gauge("cacheActivePoliticsVersion",
                () -> userFunction.cacheReady()
                        ? Long.parseLong(userFunction.currentRuntime().politicsVersion()) : -1L);
        getRuntimeContext().getMetricGroup().gauge("cachePreparationInProgress", () -> building == null ? 0 : 1);
        getRuntimeContext().getMetricGroup().gauge("cachePendingMessages", pending::size);
        getRuntimeContext().getMetricGroup().gauge("cacheProcessingEnabled", () -> ready() ? 1 : 0);
        // Restore the active version first, then prepare any saved pending update in the background.
        if (activeImage != null) startPreparation(activeImage); else startNextPreparation();
    }
    private boolean ready() { return userFunction.cacheReady(); }
    @Override public void processElement1(StreamRecord<MessageRecord> record) throws Exception {
        if (!ready() || !pending.isEmpty()) {
            addPending(MessageRecordCodec.encode(record.getValue()));
            scheduleDrain();
        } else process(record.getValue());
    }
    @Override public void processElement2(StreamRecord<CacheBundleEnvelope> record) {
        CacheBundleEnvelope envelope = record.getValue();
        envelope.validateHeader();
        if (envelope.handlerOnly) {
            userFunction.validateHandler(envelope.handler);
            selectedHandler = envelope.handler;
            // This is a mailbox callback: no executor, no RuleClassLoader, no cache rebuild.
            if (ready()) userFunction.activateHandler(selectedHandler);
            return;
        }
        // Metadata is checked here; full checksum and class loading happen on the executor.
        int rules = parseVersion(envelope.rulesVersion);
        int politics = parseVersion(envelope.politicsVersion);
        Image image = new Image(rules, politics, envelope.checksum, envelope.bytes.clone());
        Image baseline = requested != null ? requested : activeImage;
        if (baseline != null) {
            if (rules < baseline.rules || politics < baseline.politics) {
                // Old broadcast during recovery; cannot overwrite either component.
                return;
            }
            if (rules == baseline.rules && politics == baseline.politics) {
                if (!image.sameContent(baseline)) {
                    throw new IllegalStateException("Same versions, different bundle contents");
                }
                return;
            }
        }
        // Source has one Kafka partition, so events from the same source arrive ordered.
        // Each actual cache version increase may carry a handler change.
        requested = image;
        selectedHandler = envelope.handler;
        startNextPreparation();
    }

    private static int parseVersion(String version) {
        try {
            int parsed = Integer.parseInt(version);
            if (parsed < 0) throw new NumberFormatException("negative version");
            return parsed;
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid cache version: " + version, ex);
        }
    }

    private static Image newestRestored(Image old, Image candidate) {
        if (old == null) return candidate;
        if (candidate == null) return old;
        if (candidate.rules < old.rules && candidate.politics <= old.politics) return old;
        if (candidate.politics < old.politics && candidate.rules <= old.rules) return old;
        if (candidate.rules >= old.rules && candidate.politics >= old.politics) {
            if (candidate.rules == old.rules && candidate.politics == old.politics
                    && !candidate.sameContent(old)) {
                throw new IllegalStateException("Conflicting restored bundles for "
                        + candidate.versions() + "; reconcile before rescaling");
            }
            return candidate;
        }
        throw new IllegalStateException("Incomparable restored cache versions: "
                + old.versions() + " and " + candidate.versions());
    }

    private void startNextPreparation() {
        if (!closed && building == null && requested != null
                && (activeImage == null || !requested.sameContent(activeImage))) {
            startPreparation(requested);
        }
    }

    private void startPreparation(Image image) {
        building = image;
        executor.execute(() -> {
            try {
                CacheBundleState bundle = CacheBundleStateSerializer.decode(image.bytes);
                if (parseVersion(bundle.compiledRulesVersion()) != image.rules
                        || parseVersion(bundle.politicsVersion()) != image.politics
                        || !bundle.checksum().equals(image.checksum)) {
                    throw new IllegalStateException("Cache transfer identity mismatch");
                }
                PreparedRuntime prepared = testPreparer == null
                        ? userFunction.prepareBundle(bundle) : testPreparer.apply(bundle);
                post(() -> completePreparation(image, prepared, null));
            } catch (Throwable failure) {
                post(() -> completePreparation(image, null, failure));
            }
        });
    }

    private void completePreparation(Image image, PreparedRuntime prepared, Throwable failure) {
        if (closed || building != image) return;
        building = null;
        boolean superseded = requested != image;
        if (failure != null) {
            if (!superseded) {
                throw new IllegalStateException("Cache preparation failed " + image.versions(), failure);
            }
        } else if (!superseded || !ready()) {
            if (parseVersion(prepared.compiledRulesVersion()) != image.rules
                    || parseVersion(prepared.politicsVersion()) != image.politics) {
                throw new IllegalStateException("Prepared cache versions mismatch");
            }
            // A handler-only event may have arrived while this bundle was being prepared.
            // Preserve that choice without regenerating classes or indexes.
            if (selectedHandler != null) {
                userFunction.validateHandler(selectedHandler);
                prepared = new PreparedRuntime(prepared.snapshot(),
                        com.gpb.datafirewall.services.DynamicHandler.from(selectedHandler, null));
            }
            userFunction.activate(prepared);
            activeImage = image;
            scheduleDrain();
        }
        startNextPreparation();
    }

    private void post(ThrowingRunnable<? extends Exception> action) {
        if (closed) return;
        try {
            mailbox.execute(() -> { if (!closed) action.run(); }, "Publish prepared cache bundle");
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
        selectedHandlerState.update(selectedHandler == null
                ? List.of() : List.of(selectedHandler));
        activeBundleState.update(activeImage == null ? List.of() : List.of(activeImage.bytes));
        requestedBundleState.update(requested == null || (activeImage != null && requested.sameContent(activeImage))
                ? List.of() : List.of(requested.bytes));
    }
    private void stopPreparation() {
        closed = true;
        if (executor != null) executor.shutdownNow(); // never wait for a rule constructor on the mailbox
    }
    @Override public void close() throws Exception { stopPreparation(); super.close(); }
}
