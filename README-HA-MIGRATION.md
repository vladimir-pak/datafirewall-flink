# Flink 1.20 HA / checkpoint / parallelism migration

## Changes delivered

1. `src/main/java/com/gpb/datafirewall/Main.java`
   - Restart strategy and ALL checkpoint settings are now controlled exclusively by Flink's cluster `config.yaml`. Job properties named `flink.restart.*` or `flink.checkpoint.*` are ignored with a warning.
   - The job **fails fast** when `env.getCheckpointConfig().isCheckpointingEnabled()` is false. Set `execution.checkpointing.interval: 60s` in cluster `config.yaml`.
   - Maintains `parallelism.source`, `parallelism.process`, `parallelism.sink`; adds `parallelism.audit` (default = `parallelism.process`). `parallelism` is optional and, if omitted, Flink's default is retained.
   - The Kafka rules source remains parallelism 1; the broadcast rules *processing* function remains at `parallelism.process` so it can run on all processing subtasks.
   - `build-flink-audit-record-json` now starts a new chain (`startNewChain()`), with separate Flink execution thread. `flink-audit-kafka-sink` also receives configured audit parallelism.
   - Optional `audit.slot-sharing-group` (blank/unset by default) lets audit use a dedicated slot-sharing group. **This requires additional slots.**
   - Existing UIDs were preserved.
2. `src/main/java/com/gpb/datafirewall/artemis/ArtemisSource.java`
   - `RichSourceFunction` -> `RichParallelSourceFunction`. Without this change, `parallelism.source > 1` is not supported for Artemis. Its JMS connections remain local to individual subtasks.

## Example `job.properties` (add or keep alongside existing business settings)

```properties
# Optional; if absent, Flink's default parallelism is used
parallelism=4
parallelism.source=4
parallelism.process=8
parallelism.sink=4
parallelism.audit=4

# Optional only when strong resource isolation is required:
# audit.slot-sharing-group=audit

# Audit pipeline continues to use Flink checkpoint-aware KafkaSink
# NOTE: the sink is AT_LEAST_ONCE, not Kafka exactly-once transactions.
audit.kafka.enabled=true
```

Remove these obsolete job properties; their cluster config equivalents control behavior:

```properties
flink.restart.attempts
flink.restart.delay.ms
flink.checkpoint.enabled
flink.checkpoint.interval.ms
flink.checkpoint.timeout.ms
flink.checkpoint.min.pause.ms
flink.checkpoint.max.concurrent
```

## Example effective Flink cluster config

Keep the user's ZooKeeper/S3/restart/checkpoint configuration in `config.yaml` on each Flink cluster node. Relevant sections:

```yaml
restart-strategy:
  type: exponential-delay
  exponential-delay:
    initial-backoff: 5s
    max-backoff: 2min
    backoff-multiplier: 2.0
    reset-backoff-threshold: 10min
    jitter-factor: 0.1
execution:
  checkpointing:
    interval: 60s
    timeout: 2min
    min-pause: 10s
    max-concurrent-checkpoints: 1
    tolerable-failed-checkpoints: 3
    externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
    mode: EXACTLY_ONCE
    unaligned:
      enabled: true
    aligned-checkpoint-timeout: 30s
    storage: filesystem
    dir: s3://flink/ha/checkpoints
    savepoint-dir: s3://flink/ha/savepoints
    incremental: true
    num-retained: 3
    local-backup:
      enabled: true
state:
  backend:
    type: rocksdb
```

## Important: not yet end-to-end reliable MQ request/reply

Flink EXACTLY_ONCE is a checkpoint/state consistency setting **not** a distributed transaction between MQ GET, MQ PUT and Kafka audit. Current connectors:

- `MqSource`: reads using `queue.get(msg, gmo)` with no `MQGMO_SYNCPOINT` and no checkpoint/commit integration; it consumes before reply is durably written.
- `MqSink`: `MQPMO_NO_SYNCPOINT`, not transactionally coupled to MQ Source.
- `ArtemisSource`: `Session.AUTO_ACKNOWLEDGE`; acknowledgment is not tied to finishing processing and sending reply.
- `RulesReloadBroadcastProcessFunction`: some exception paths log errors without propagating failures, so a missing output may not restart the operator.
- Audit serializer: exception is logged and the record dropped; the Kafka Sink delivery guarantee is `AT_LEAST_ONCE` and may duplicate records.

**Do not claim guaranteed redelivery, exactly-once MQ request/reply or durable audit from the Main-only patch.** For that, design a coordinated MQ transaction/ack protocol and idempotent reply handling (or durable inbox/outbox). A Source-only syncpoint does not allow an MQ Sink subtask to atomically commit a GET in another MQ connection/transaction. A source checkpoint alone does not commit after the downstream MQ Sink succeeds.

Chaining separation also does **not** remove network backpressure. A slow or unavailable Kafka audit sink can still fill buffers, block the side-output producer and slow requests. If the user truly needs Kafka audit to be independently delayed/failed without slowing MQ reply, add a durable, bounded outbox or a second job consuming a durable topic/queue. A plain untracked ExecutorService is not sufficient.

### Existing state and rollout

All old `.uid()` values were preserved: `mq-source`, `artemis-source`, `rules-kafka-source`, `process-with-rules-reload`, `build-short-reply`, `mq-sink`, `artemis-sink`, `build-flink-audit-record-json`, `flink-audit-kafka-sink`. The broadcast state's descriptor name `cache-update-broadcast` stays intact. Before changing job topology or operator parallelism, trigger a savepoint and test restore in staging; upgrade paths involving legacy sources/sink and Kafka sink writers must be verified against your existing savepoint.

### Validation checklist

1. Start without `execution.checkpointing.interval`: submission should fail immediately with an actionable error, before Vault initialization.
2. Start with interval 60s: check logs for `mode=EXACTLY_ONCE`, `intervalMs=60000`, timeout and other inherited values. Verify the UI's Checkpoints tab.
3. Configure `parallelism.source=4`, `parallelism.process=8`, `parallelism.sink=4`, `parallelism.audit=4`: check Kafka rules source runs with 1, process with 8, audit mapper/sink with 4.
4. Inspect job graph: `build-flink-audit-record-json` starts a separate chain from `process-with-rules-reload`. No additional slot-sharing group unless configured.
5. Test MQ request replies and audit with the Kafka broker running and unavailable. With broker unavailable, expect backpressure; this patch does not guarantee reply latency in that condition.
6. Trigger savepoint, cancel, restore. Verify cache-rule updates from Kafka and correct message handling after restart. Use a separate non-production queue for failure injection.
7. Test loss of TaskManager before, during, after MQ PUT. Without an ack/syncpoint redesign, automatic replay is **not** guaranteed.

### Build limitations

The uploaded archive contained `src/` only, without `pom.xml`/Gradle wrapper/dependency lockfiles. The changed sources have been inspected statically but not compiled or executed against the user's Flink 1.20 dependency set. Use the actual project build to check API compatibility before deployment.
