package com.gpb.datafirewall.transaction.kafka;

import com.gpb.datafirewall.audit.AuditConfig;
import com.gpb.datafirewall.bundle.CacheBundleEnvelope;
import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.DynamicHandler;
import com.gpb.datafirewall.services.PreparedRuntime;
import com.gpb.datafirewall.services.RulesProcessingFunction;
import com.gpb.datafirewall.transaction.BrokerWorker;
import com.gpb.datafirewall.transaction.TransactionSettings;
import com.gpb.datafirewall.transaction.TransactionalReply;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import java.util.Map;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache input and managed state run on the mailbox. A dedicated broker worker owns
 * blocking receive, processing, reply and commit. Each request captures one immutable
 * runtime. Only bounded best-effort post-commit audit is handed back to the mailbox.
 */
public final class TransactionalRulesOperator
        extends AbstractUdfStreamOperator<ProcessingResult, RulesProcessingFunction>
        implements OneInputStreamOperator<CacheBundleEnvelope, ProcessingResult> {

    private static final Logger log = LoggerFactory.getLogger(TransactionalRulesOperator.class);

    private final MailboxExecutor mailbox;
    private final TransactionSettings settings;
    private final AuditConfig auditConfig;
    private transient volatile BrokerWorker brokerWorker;
    private transient MailboxAuditBuffer auditBuffer;
    private transient ExecutorService prepareExecutor;
    private transient ListState<byte[]> activeState;
    private transient ListState<byte[]> requestedState;
    private transient ListState<String> selectedHandlerState;
    private Image activeImage;
    private Image requested;
    private volatile Image building;
    private String selectedHandler;
    private volatile boolean closed;

    private record Image(int rules, int politics, String checksum, byte[] bytes) {
        static Image decode(byte[] source) {
            byte[] bytes = source.clone();
            CacheBundleState bundle = CacheBundleStateSerializer.decode(bytes);
            return new Image(version(bundle.compiledRulesVersion()),
                    version(bundle.politicsVersion()), bundle.checksum(), bytes);
        }

        boolean same(Image other) {
            return other != null && Objects.equals(checksum, other.checksum);
        }

        String versions() {
            return "rules=" + rules + ",politics=" + politics;
        }
    }

    public TransactionalRulesOperator(RulesProcessingFunction function,
            MailboxExecutor mailbox, TransactionSettings settings, AuditConfig auditConfig) {
        super(function);
        this.mailbox = mailbox;
        this.settings = settings;
        this.auditConfig = auditConfig;
    }

    private static int version(String value) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) throw new IllegalArgumentException("Negative cache version");
            return parsed;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid cache version: " + value, e);
        }
    }

    private static Image newest(Image previous, Image candidate) {
        if (previous == null) return candidate;
        if (candidate == null) return previous;
        if (candidate.rules < previous.rules || candidate.politics < previous.politics) {
            if (candidate.rules <= previous.rules && candidate.politics <= previous.politics) {
                return previous;
            }
            throw new IllegalStateException("Incomparable cache versions: "
                    + previous.versions() + " and " + candidate.versions());
        }
        if (candidate.rules == previous.rules && candidate.politics == previous.politics) {
            if (!candidate.same(previous)) {
                throw new IllegalStateException("Same cache versions with different contents: "
                        + candidate.versions());
            }
            return previous;
        }
        return candidate;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        activeState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "broker-local-active-v3", BytePrimitiveArraySerializer.INSTANCE));
        requestedState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "broker-local-requested-v3", BytePrimitiveArraySerializer.INSTANCE));
        selectedHandlerState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "broker-local-handler-v3", StringSerializer.INSTANCE));
        if (context.isRestored()) {
            for (byte[] bytes : activeState.get()) activeImage = newest(activeImage, Image.decode(bytes));
            requested = activeImage;
            for (byte[] bytes : requestedState.get()) requested = newest(requested, Image.decode(bytes));
            for (String value : selectedHandlerState.get()) {
                if (selectedHandler != null && !selectedHandler.equals(value)) {
                    throw new IllegalStateException("Conflicting handlers during rescale");
                }
                selectedHandler = value;
            }
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        prepareExecutor = Executors.newSingleThreadExecutor(task -> {
            Thread worker = new Thread(task,
                    "cache-prepare-tx-" + getRuntimeContext().getIndexOfThisSubtask());
            worker.setDaemon(true);
            return worker;
        });
        // The initial restored cache must be active before we accept any broker input.
        // This step does not access the API and happens only during task startup.
        if (activeImage != null) {
            PreparedRuntime restored = userFunction.prepareBundle(
                    CacheBundleStateSerializer.decode(activeImage.bytes));
            userFunction.activate(withSelectedHandler(restored));
        }
        var global = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ParameterTool pt = ParameterTool.fromMap(global == null ? Map.of() : global.toMap());
        auditBuffer = new MailboxAuditBuffer(mailbox,
                result -> output.collect(new StreamRecord<>(result)), this::failAsync,
                pt.getInt("transaction.audit.buffer.max-records", 1024),
                pt.getLong("transaction.audit.buffer.max-bytes", 16L * 1024 * 1024));
        startBrokerIfReady();
        startNextPreparation();
        getRuntimeContext().getMetricGroup().gauge("brokerCacheReady", () -> ready() ? 1 : 0);
        getRuntimeContext().getMetricGroup().gauge("brokerCachePreparing", () -> building == null ? 0 : 1);
        getRuntimeContext().getMetricGroup().gauge("brokerConnected",
                () -> brokerWorker != null && brokerWorker.isConnected() ? 1 : 0);
        getRuntimeContext().getMetricGroup().gauge("brokerProcessing",
                () -> brokerWorker != null && brokerWorker.isProcessing() ? 1 : 0);
        getRuntimeContext().getMetricGroup().gauge("brokerRepliesCommitted",
                () -> brokerWorker == null ? 0L : brokerWorker.repliesCommitted());
        getRuntimeContext().getMetricGroup().gauge("brokerAuditPending", auditBuffer::size);
        getRuntimeContext().getMetricGroup().gauge("brokerAuditPendingBytes", auditBuffer::retainedBytes);
        getRuntimeContext().getMetricGroup().gauge("brokerAuditDropped", auditBuffer::dropped);
    }

    private boolean ready() {
        return userFunction.cacheReady();
    }

    private void startBrokerIfReady() {
        if (closed || brokerWorker != null || !ready()) return;
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        brokerWorker = new BrokerWorker("broker-receiver-" + subtask + "-"
                + getRuntimeContext().getAttemptNumber(),
                () -> settings.connect(auditConfig, subtask, parallelism), record -> {
                    ProcessingResult[] reply = new ProcessingResult[1];
                    ProcessingResult[] audit = new ProcessingResult[1];
                    // One receiver processes serially. process() captures a complete runtime once.
                    userFunction.process(record, result -> reply[0] = result, result -> audit[0] = result);
                    return new TransactionalReply(reply[0], audit[0]);
                }, auditBuffer::offer, this::failAsync);
        brokerWorker.start();
    }

    private void failAsync(Throwable failure) {
        if (!closed) getContainingTask().handleAsyncException("Transactional broker worker failed", failure);
    }

    /** The only Flink input contains actual cache updates, never poll ticks. */
    @Override
    public void processElement(StreamRecord<CacheBundleEnvelope> record) {
        CacheBundleEnvelope update = record.getValue();
        if (update == null) return;
        update.validateHeader();
        if (update.handlerOnly) {
            userFunction.validateHandler(update.handler);
            selectedHandler = update.handler;
            if (ready()) userFunction.activateHandler(selectedHandler);
            return;
        }
        Image image = new Image(version(update.rulesVersion), version(update.politicsVersion),
                update.checksum, update.bytes.clone());
        Image current = requested != null ? requested : activeImage;
        Image latest = newest(current, image);
        if (latest != image) return;
        requested = image;
        selectedHandler = update.handler;
        startNextPreparation();
    }

    private PreparedRuntime withSelectedHandler(PreparedRuntime runtime) {
        if (selectedHandler == null) return runtime;
        DynamicHandler handler = userFunction.validateHandler(selectedHandler);
        return runtime.handler() == handler
                ? runtime : new PreparedRuntime(runtime.snapshot(), handler);
    }

    private void startNextPreparation() {
        if (closed || prepareExecutor == null || building != null || requested == null
                || requested.same(activeImage)) return;
        Image target = requested;
        building = target;
        try {
            prepareExecutor.execute(() -> {
                try {
                    CacheBundleState bundle = CacheBundleStateSerializer.decode(target.bytes);
                    if (version(bundle.compiledRulesVersion()) != target.rules
                            || version(bundle.politicsVersion()) != target.politics
                            || !bundle.checksum().equals(target.checksum)) {
                        throw new IllegalStateException("Cache bundle identity mismatch");
                    }
                    PreparedRuntime prepared = userFunction.prepareBundle(bundle);
                    postCompletion(target, prepared, null);
                } catch (Throwable failure) {
                    postCompletion(target, null, failure);
                }
            });
        } catch (RejectedExecutionException e) {
            building = null;
            throw new IllegalStateException("Cache preparation rejected", e);
        }
    }

    private void postCompletion(Image target, PreparedRuntime prepared, Throwable failure) {
        if (closed) return;
        try {
            mailbox.execute(() -> completePreparation(target, prepared, failure),
                    "activate local cache %s", target.versions());
        } catch (RejectedExecutionException rejected) {
            if (!closed) getContainingTask().handleAsyncException(
                    "Could not publish prepared cache", rejected);
        }
    }

    private void completePreparation(Image target, PreparedRuntime prepared, Throwable failure) {
        if (closed || building != target) return;
        building = null;
        boolean stale = requested != target;
        if (failure != null) {
            if (!stale) {
                throw new IllegalStateException("Cache preparation failed: " + target.versions(), failure);
            }
        } else if (!stale) {
            if (version(prepared.compiledRulesVersion()) != target.rules
                    || version(prepared.politicsVersion()) != target.politics) {
                throw new IllegalStateException("Prepared cache version mismatch");
            }
            userFunction.activate(withSelectedHandler(prepared));
            activeImage = target;
            startBrokerIfReady();
            log.info("Activated rules={} politics={} handler={} subtask={}",
                    target.rules, target.politics, userFunction.currentRuntime().handler(),
                    getRuntimeContext().getIndexOfThisSubtask());
        }
        startNextPreparation();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        activeState.update(activeImage == null ? List.of() : List.of(activeImage.bytes));
        requestedState.update(requested == null || requested.same(activeImage)
                ? List.of() : List.of(requested.bytes));
        selectedHandlerState.update(selectedHandler == null ? List.of() : List.of(selectedHandler));
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (prepareExecutor != null) prepareExecutor.shutdownNow();
        if (auditBuffer != null) auditBuffer.close();
        try {
            if (brokerWorker != null) {
                brokerWorker.close();
                if (!brokerWorker.awaitTermination(Math.min(settings.receiveTimeoutMs() + 1000L, 10_000L))) {
                    log.warn("Broker worker is still stopping; its owner will rollback/close when I/O returns");
                }
            }
        } finally {
            super.close();
        }
    }
}
