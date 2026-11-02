package com.gpb.datafirewall.coordination;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Activate;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Activated;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Assembler;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Chunk;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Hello;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Pause;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Paused;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Ready;
import com.gpb.datafirewall.coordination.CacheActivationEvent.Resume;
import com.gpb.datafirewall.dto.ProcessingResult;
import com.gpb.datafirewall.services.MessageRecord;
import com.gpb.datafirewall.services.MessageRecordCodec;
import com.gpb.datafirewall.services.RulesProcessingFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.rmi.server.UID;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import static com.gpb.datafirewall.coordination.CacheActivationEvent.*;

public final class RulesProcessingOperator
        extends AbstractUdfStreamOperator<ProcessingResult, RulesProcessingFunction>
        implements TwoInputStreamOperator<MessageRecord, CacheBundleState, ProcessingResult>,
                   OperatorEventHandler {

    private final OperatorEventGateway gateway;
    private final MailboxExecutor mailbox;
    private final int maxPending;
    private final long maxPendingBytes;
    private final Deque<byte[]> pending = new ArrayDeque<>();
    private final Assembler transfer = new Assembler();
    private transient ListState<byte[]> pendingState;
    private long pendingBytes;
    private boolean accepting;
    private boolean drainScheduled;
    private long preparedEpoch = -1;
    private String preparedChecksum;

    public RulesProcessingOperator(
            RulesProcessingFunction function, 
            OperatorEventGateway gateway,
            MailboxExecutor mailbox, 
            int maxPending, 
            long maxPendingBytes) {

        super(function);
        this.gateway = gateway;
        this.mailbox = mailbox;
        this.maxPending = maxPending;
        this.maxPendingBytes = maxPendingBytes;
    }

    @Override 
    public void initializeState(
            StateInitializationContext context) throws Exception {
        super.initializeState(context);

        pendingState = context
                .getOperatorStateStore()
                .getListState(
                        new ListStateDescriptor<>(
                                "cache-startup-pending-v1", 
                                BytePrimitiveArraySerializer.INSTANCE
                        )
                );

        if (context.isRestored()) 
            for (byte[] message : pendingState.get()) 
                addPending(message);

        // Сохраняем старый формат UID оператора и сообщений, ожидающих обработки, 
        // для обеспечения совместимости при обновлении.
        // Старый broadcast copy избыточен
        // Восстановленный источник предоставляет полный набор данных, 
        // а новый координатор фиксирует его состояние (создает checkpoint) перед тем, 
        // как разрешить дальнейшую обработку.
        context
                .getOperatorStateStore()
                .getBroadcastState(
                        new org.apache.flink.api.common.state.MapStateDescriptor<>(
                                "cache-bundle-broadcast-v1", 
                                org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                                new CacheBundleStateSerializer()
                        )
                ).clear();

        // Не запускать с локальным дефолтным epoch
        accepting = false;
    }

    @Override 
    public void open() throws Exception {
        super.open();

        getRuntimeContext()
                .getMetricGroup()
                .gauge("cacheActiveEpoch", () -> userFunction.activeEpoch());
        getRuntimeContext()
                .getMetricGroup()
                .gauge("cachePendingMessages", pending::size);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("cacheProcessingEnabled", () -> accepting ? 1 : 0);

        gateway.sendEventToCoordinator(new Hello());
    }

    @Override 
    public void processElement1(
            StreamRecord<MessageRecord> record) throws Exception {
        if (!accepting || !pending.isEmpty()) {
            addPending(MessageRecordCodec.encode(record.getValue()));
            scheduleDrain();
        } else {
            process(record.getValue());
        }
    }

    @Override 
    public void processElement2(
            StreamRecord<CacheBundleState> record) {
        if (getRuntimeContext().getIndexOfThisSubtask() != 0)
            throw new IllegalStateException(
                    "Cache proposals must be globally partitioned to subtask 0"
            );

        CacheBundleState bundle = record.getValue();
        sendBundle(
                bundle.cacheEpoch(), 
                bundle.checksum(), 
                CacheBundleStateSerializer.encode(bundle),
                gateway::sendEventToCoordinator
        );
    }

    @Override 
    public void handleOperatorEvent(OperatorEvent event) {
        try {
            if (event instanceof Chunk chunk) {
                byte[] bytes = transfer.accept(chunk);
                if (bytes != null) {
                    CacheBundleState bundle = CacheBundleStateSerializer.decode(bytes);
                    if (bundle.cacheEpoch() != chunk.epoch() || !bundle.checksum().equals(chunk.checksum()))
                        throw new IllegalStateException("Cache transfer identity mismatch");
                    if (bundle.cacheEpoch() < userFunction.activeEpoch())
                        throw new IllegalStateException("Cache rollback outside checkpoint recovery");
                    userFunction.prepare(bundle);
                    preparedEpoch = bundle.cacheEpoch();
                    preparedChecksum = bundle.checksum();
                    gateway.sendEventToCoordinator(new Ready(preparedEpoch, preparedChecksum));
                }
            } else if (event instanceof Pause pause) {
                requirePrepared(pause.epoch(), pause.checksum());
                // All previously started synchronous processing has returned before this callback.
                accepting = false;
                gateway.sendEventToCoordinator(new Paused(preparedEpoch, preparedChecksum));
            } else if (event instanceof Activate activate) {
                requirePrepared(activate.epoch(), activate.checksum());
                if (accepting) throw new IllegalStateException("Activation while processing is enabled");
                userFunction.activate(preparedEpoch, preparedChecksum);
                gateway.sendEventToCoordinator(new Activated(preparedEpoch, preparedChecksum));
            } else if (event instanceof Resume resume) {
                requirePrepared(resume.epoch(), resume.checksum());
                if (userFunction.activeEpoch() != resume.epoch())
                    throw new IllegalStateException("Resume before activation");
                accepting = true;
                scheduleDrain();
            } else {
                throw new IllegalArgumentException("Unexpected coordinator event " + event);
            }
        } catch (Exception failure) {
            throw new IllegalStateException("Cache coordination failed", failure);
        }
    }

    private void requirePrepared(long epoch, String checksum) {
        if (epoch != preparedEpoch || !java.util.Objects.equals(checksum, preparedChecksum))
            throw new IllegalStateException("Unknown prepared cache epoch=" + epoch);
    }

    private void addPending(byte[] bytes) {
        if (pending.size() >= maxPending 
                || bytes.length > maxPendingBytes - pendingBytes)
            throw new IllegalStateException(
                    "Cache message buffer limit exceeded; messages=" 
                    + pending.size()
                    + " bytes=" + pendingBytes 
                    + ". Size cache.activation.max-pending-* for activation pauses."
            );

        pending.addLast(bytes);
        pendingBytes += bytes.length;
    }

    private void scheduleDrain() {
        if (!accepting || pending.isEmpty() || drainScheduled) return;
        drainScheduled = true;
        mailbox.execute(() -> {
            drainScheduled = false;
            // Yield between small batches so barriers and PAUSE are never starved by the backlog.
            for (int i = 0; i < 64 && accepting && !pending.isEmpty(); i++) {
                byte[] bytes = pending.peekFirst();
                process(MessageRecordCodec.decode(bytes));
                pending.removeFirst();
                pendingBytes -= bytes.length;
            }
            scheduleDrain();
        }, "Drain messages after coordinated cache activation");
    }

    private void process(MessageRecord record) {
        userFunction.process(record,
                result -> output.collect(new StreamRecord<>(result)),
                result -> output.collect(RulesProcessingFunction.FLINK_AUDIT_RESULT_TAG,
                        new StreamRecord<>(result)));
    }

    @Override 
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        pendingState.update(new ArrayList<>(pending));
    }
}
