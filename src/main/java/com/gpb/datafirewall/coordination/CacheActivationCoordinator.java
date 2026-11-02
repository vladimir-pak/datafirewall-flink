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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static com.gpb.datafirewall.coordination.CacheActivationEvent.*;

/**
 * Доступ ко всему изменяемому состоянию протокола осуществляется в основном потоке JobManager.
 * Завершенная контрольная точка принятия решения — это точка фиксации (commit point), предшествующая отправке команды ACTIVATE.
 * Сбой в работе worker намеренно переводит систему в режим глобального восстановления: смешанные epochs не допускаются.
 */
public final class CacheActivationCoordinator implements OperatorCoordinator {
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(CacheActivationCoordinator.class);
            
    private enum Phase { IDLE, PREPARING, PAUSING, WAITING_CHECKPOINT, ACTIVATING }
    private static final int MAGIC = 0x43414331;
    private final Context context;
    private final Map<Integer, SubtaskGateway> gateways = new HashMap<>();
    private final Map<Integer, Integer> attempts = new HashMap<>();
    private final Set<Integer> online = new HashSet<>();
    private final Set<Integer> acknowledgements = new HashSet<>();
    private final Set<Long> decisionCheckpoints = new HashSet<>();
    private Assembler proposal = new Assembler();
    private Phase phase = Phase.IDLE;
    private Bundle committed;
    private Bundle candidate;
    private Bundle queued;
    private boolean restoring;
    private boolean failing;

    private record Bundle(long epoch, String checksum, byte[] bytes) {}

    public CacheActivationCoordinator(Context context) {
        this.context = context;
    }

    @Override public void start() {
        if (context.isConcurrentExecutionAttemptsSupported())
            throw new IllegalStateException("Speculative execution is unsupported for cache activation");
    }
    @Override public void close() {}

    @Override public void executionAttemptReady(int subtask, int attempt, SubtaskGateway gateway) {
        gateways.put(subtask, gateway);
        attempts.put(subtask, attempt);
        maybeStart();
    }

    @Override public void handleEventFromOperator(int subtask, int attempt, OperatorEvent event) {
        if (failing || !Objects.equals(attempts.get(subtask), attempt)) return;
        if (event instanceof Hello) {
            online.add(subtask);
            maybeStart();
        } else if (event instanceof Chunk chunk) {
            if (subtask != 0) throw new IllegalStateException("Only subtask 0 may propose a cache");
            byte[] bytes = proposal.accept(chunk);
            if (bytes != null) {
                // Worker 0 has already validated/serialized this bundle. Verify again on workers,
                // keeping class loading and full hashing off the JobManager thread.
                offer(new Bundle(chunk.epoch(), chunk.checksum(), bytes));
            }
        } else if (event instanceof Ready ready) {
            acknowledge(subtask, Phase.PREPARING, ready.epoch(), ready.checksum());
        } else if (event instanceof Paused paused) {
            acknowledge(subtask, Phase.PAUSING, paused.epoch(), paused.checksum());
        } else if (event instanceof Activated activated) {
            acknowledge(subtask, Phase.ACTIVATING, activated.epoch(), activated.checksum());
        } else {
            throw new IllegalArgumentException("Unexpected cache event " + event);
        }
    }

    private void offer(Bundle bundle) {
        if (bundle.epoch < 0 || bundle.checksum == null) throw new IllegalArgumentException("Bad proposal");
        for (Bundle known : new Bundle[]{committed, candidate, queued}) {
            if (known != null && known.epoch == bundle.epoch) {
                if (!known.checksum.equals(bundle.checksum))
                    throw new IllegalStateException("Different contents for epoch " + bundle.epoch);
                return;
            }
        }
        long floor = Math.max(committed == null ? -1 : committed.epoch,
                candidate == null ? -1 : candidate.epoch);
        if (bundle.epoch <= floor) return;
        if (queued == null || bundle.epoch > queued.epoch) queued = bundle;
        maybeStart();
    }

    private void maybeStart() {
        if (failing || phase != Phase.IDLE || online.size() != context.currentParallelism()
                || gateways.size() != context.currentParallelism()) return;
        if (restoring && committed != null) {
            candidate = committed;
        } else {
            restoring = false;
            if (queued == null) return;
            candidate = queued;
            queued = null;
        }
        phase = Phase.PREPARING;
        log.info("[CACHE-COORD] preparing epoch={} workers={} restoring={}",
                candidate.epoch, context.currentParallelism(), restoring);
        acknowledgements.clear();
        for (SubtaskGateway gateway : gateways.values())
            sendBundle(candidate.epoch, candidate.checksum, candidate.bytes, event -> send(gateway, event));
    }

    private void acknowledge(int subtask, Phase expected, long epoch, String checksum) {
        if (candidate == null || epoch < candidate.epoch) return; // stale acknowledgements
        if (epoch != candidate.epoch || !candidate.checksum.equals(checksum) || phase != expected)
            throw new IllegalStateException("Unexpected acknowledgement epoch=" + epoch + " phase=" + phase);
        acknowledgements.add(subtask);
        if (acknowledgements.size() != context.currentParallelism()) return;
        acknowledgements.clear();
        if (expected == Phase.PREPARING) {
            if (restoring) {
                phase = Phase.ACTIVATING;
                broadcast(new Activate(candidate.epoch, candidate.checksum));
            } else {
                phase = Phase.PAUSING;
                broadcast(new Pause(candidate.epoch, candidate.checksum));
            }
        } else if (expected == Phase.PAUSING) {
            phase = Phase.WAITING_CHECKPOINT;
            log.info("[CACHE-COORD] all workers paused, awaiting decision checkpoint for epoch={}", candidate.epoch);
            // Non-blocking request; periodic checkpoints retry if this request is declined.
            if (context.getCheckpointCoordinator() != null)
                context.getCheckpointCoordinator().triggerCheckpoint(false);
        } else if (expected == Phase.ACTIVATING) {
            log.info("[CACHE-COORD] all workers activated epoch={}, resuming processing", candidate.epoch);
            broadcast(new Resume(candidate.epoch, candidate.checksum));
            committed = candidate;
            candidate = null;
            phase = Phase.IDLE;
            restoring = false;
            maybeStart();
        }
    }

    private void broadcast(OperatorEvent event) {
        for (SubtaskGateway gateway : gateways.values()) send(gateway, event);
    }
    private void send(SubtaskGateway gateway, OperatorEvent event) {
        // Event delivery is fenced by Flink at the coordinator checkpoint boundary.
        // Flink fails the task for delivery errors; executionAttemptFailed escalates recovery.
        gateway.sendEvent(event);
    }

    @Override public void checkpointCoordinator(long id, CompletableFuture<byte[]> result) {
        try {
            Bundle durable = phase == Phase.WAITING_CHECKPOINT ? candidate : committed;
            if (phase == Phase.WAITING_CHECKPOINT) decisionCheckpoints.add(id);
            // No events are emitted until after completing this future on the same thread.
            result.complete(encode(durable));
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
        }
    }

    @Override public void notifyCheckpointComplete(long id) {
        if (!failing && phase == Phase.WAITING_CHECKPOINT && decisionCheckpoints.contains(id)) {
            committed = candidate;
            log.info("[CACHE-COORD] decision checkpoint={} completed for epoch={}", id, candidate.epoch);
            decisionCheckpoints.clear();
            phase = Phase.ACTIVATING;
            broadcast(new Activate(candidate.epoch, candidate.checksum));
        }
    }
    @Override public void notifyCheckpointAborted(long id) { decisionCheckpoints.remove(id); }

    @Override public void resetToCheckpoint(long id, byte[] state) throws Exception {
        committed = decode(state);
        candidate = null; queued = null;
        phase = Phase.IDLE;
        restoring = committed != null;
        failing = false;
        gateways.clear(); attempts.clear(); online.clear(); acknowledgements.clear();
        decisionCheckpoints.clear(); proposal = new Assembler();
    }

    @Override public void executionAttemptFailed(int subtask, int attempt, Throwable reason) {
        if (Objects.equals(attempts.get(subtask), attempt)) {
            gateways.remove(subtask); attempts.remove(subtask); online.remove(subtask);
            failGlobally("Cache worker failed; restore every worker to the same decision", reason);
        }
    }
    @Override public void subtaskReset(int subtask, long checkpoint) {
        failGlobally("Regional cache recovery requires a global checkpoint restore", null);
    }
    private void failGlobally(String message, Throwable cause) {
        if (!failing) {
            failing = true;
            context.failJob(new IllegalStateException(message, cause));
        }
    }

    private static byte[] encode(Bundle bundle) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(MAGIC);
        out.writeInt(bundle == null ? 0 : bundle.bytes.length);
        if (bundle != null) out.write(bundle.bytes);
        return buffer.toByteArray();
    }
    private static Bundle decode(byte[] state) throws IOException {
        if (state == null) return null;
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(state));
        if (in.readInt() != MAGIC) throw new IOException("Unknown cache coordinator state");
        int length = in.readInt();
        if (length < 0 || length > MAX_BUNDLE_BYTES) throw new IOException("Bad coordinator bundle length");
        byte[] bytes = new byte[length]; in.readFully(bytes);
        if (in.available() != 0) throw new IOException("Trailing coordinator state");
        if (length == 0) return null;
        CacheBundleState bundle = CacheBundleStateSerializer.decode(bytes);
        return new Bundle(bundle.cacheEpoch(), bundle.checksum(), bytes);
    }

    public record Provider(OperatorID operatorId) implements OperatorCoordinator.Provider {
        @Override public OperatorID getOperatorId() { return operatorId; }
        @Override public OperatorCoordinator create(Context context) {
            return new CacheActivationCoordinator(context);
        }
    }
}
