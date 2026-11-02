# Flink 1.20 cache bundle migration (2026-10-30)

## Source changes

- `bundle/CacheBundleState`: portable immutable full-bundle DTO. Does not serialize dynamically instantiated `Rule` objects, class loaders, HTTP clients, etc. `compiledRulesVersion` and `politicsVersion` are independent; `cacheEpoch` names the consistent combination.
- `bundle/CacheBundleStateSerializer`: deterministic binary v1, version marker, canonical sorted maps/sets, UTF-8 strings and SHA-256 of canonical payload. `CacheBundleStateTypeInfo` binds the explicit serializer to the data stream, and a `MapStateDescriptor` binds it to Flink Broadcast State.
- `source/CacheSnapshotSource`: single nonparallel source with Kafka consumer assigned to exactly one partition; capture Kafka end offset **before** cold API bootstrap. Then load epoch manifest and concrete versioned caches, emit full bundle. On restart, emit restored bundle without API calls and seek to saved offset. Save `nextOffset`, `pendingEpoch`, `pendingOffset` and full bundle in explicit byte[] ListState. Pending update is replayed from `nextOffset` after restore; offsets are not auto-committed. On fetch failures, keep the previous bundle and retry the failed epoch with backoff.
- `CacheBootstrapService` now downloads a concrete immutable epoch only, from a manifest, and verifies published checksum.
- `RulesCacheRuntime` has `prepare()`, `activate()`, `snapshot()`; no API access. `RulesReloader.loadFromBytecodes()` loads locally, without modifying active registry. `RulesCacheSnapshot` deep-freezes nested data and tracks cacheEpoch.
- `RulesReloadBroadcastProcessFunction` broadcasts complete bundles, never calls API and lazily reconstructs the local snapshot from restored Broadcast State on the first input. The process operator also keeps a bounded checkpointed startup buffer for requests that arrive before the initial broadcast. Handler is routed as part of the same bundle.
- `CacheUpdateEvent` and deserializer accept `cacheEpoch` in Kafka JSON. Old `{version,cacheName}` events are decoded but rejected by the new source (fail closed, not silently skipped).

## Required Spring API change (NOT included in this ZIP)

This is a **coordinated deployment**; the existing cache server cannot serve the manifest endpoint until you implement it. Publish a consistent **immutable epoch manifest** with these endpoints:

```
GET /api/v1/cache/bundle-manifest/latest
GET /api/v1/cache/bundle-manifest/{cacheEpoch}
```

Example response:

```json
{
  "cacheEpoch": 72,
  "compiledRulesVersion": "71",
  "politicsVersion": "65",
  "handler": "flink",
  "checksum": "<sha256 of canonical CacheBundleStateSerializer payload>"
}
```

**Checksum contract:** `SHA-256` of `CacheBundleStateSerializer.payload()` (the deterministic, sorted, length-prefixed binary payload) across the complete set of 8 caches + metadata. To compute it on Spring, share the portable DTO plus canonical checksum code, or implement precisely the same format there. If Spring cannot provide checksum immediately, set `cache.manifest.require-publisher-checksum=false` temporarily: this still computes and verifies the checksum on broadcast/state load, **but does not verify fidelity against the publisher**.

Publish all versioned caches first (they MUST remain immutable and available while the epoch is in flight), then publish the manifest by atomically updating the `latest` pointer, then publish Kafka notification:

```json
{"cacheEpoch":72}
```

`GET /api/v1/cache/compiled_rules_71` and all seven versioned politics endpoints must remain available. A cache epoch may use two different version numbers. No `/latest` calls per component. Never delete versioned cache entries referenced by a manifest while a consumer might still reload them after failure; retain sufficiently many epochs or use GC governed by subscriber/checkpoint acknowledgement.

## job.properties

```
cache.bootstrap.enabled=true
rules.loader=http
ignite.apiUrl=https://cache-api.example.local
cache.manifest.require-publisher-checksum=true
cache.reload.retry.initial.ms=5000
cache.reload.retry.max.ms=60000
cache.startup.max-pending-messages=10000
```

`rules.loader=thin` also works for rule bytecodes, but the metadata/politics API still uses HTTPS. `parallelism.process` remains independent; source is **always parallelism=1**. `parallelism.audit`, MQ sink/source, UID of other compatible operators unchanged.

## Restore behavior / constraints

- With checkpoint enabled: source ListState + Broadcast State are recovered from Flink checkpoint in S3; local executable rule classes are reconstructed from restored bytes. Cache API can be unavailable on **restore** and processing can still use existing state; later updates will be retried.
- With checkpoint disabled: source starts from scratch, first captures Kafka tail offset and then loads latest manifest; there is no guarantee of restoring an old snapshot from Flink after a crash.
- If a crash happens before the FIRST successful checkpoint, bootstrap API is required again.
- Kafka partition count must be exactly 1; Kafka offsets are owned by Flink operator state, NOT Kafka group commits.
- **Cold start ordering:** regular MQ inputs can reach the processor before its first Broadcast State update. `RulesReloadBroadcastProcessFunction` buffers these inputs in bounded managed `ListState<byte[]>` and drains them when a bundle arrives; max pending is `cache.startup.max-pending-messages` (default 10000 per subtask). Overflow fails closed. If a crash happens before the FIRST successful checkpoint, even buffered MQ messages are not recoverable through Flink. IBM MQ/JMS acknowledgement is still not checkpoint-coordinated; for a strict no-loss service implement transactional MQ acknowledgement / source gating BEFORE production.
- The old `rules-kafka-source` UID and `cache-update-broadcast` state are INCOMPATIBLE with the new bundle type. New UIDs are `cache-snapshot-source-v1` and `process-with-cache-bundle-v1`. This is a **state migration boundary**: do not assume an old savepoint restores directly. Preserve other compatible operator UIDs, and migrate using a planned fresh start or explicit state migration / `--allowNonRestoredState` after reviewing MQ in-flight semantics.
- The source is implemented with deprecated `SourceFunction` / `CheckpointedFunction` APIs supported in Flink 1.20 for minimal integration. A future FLIP-27 Source implementation would need its own enumerator/split and serializer design.
- Full bundles are checkpointed in the source and copied into broadcast state per processor subtask. Expect checkpoint size / broadcast network traffic to scale with compiled bytecode size and `parallelism.process`. Measure before production.

## Validation

Added `CacheBundleStateSerializerTest.java` for deterministic round-trip, immutability and corruption detection. Run in the full project build (`mvn test`). This archive contains only `src/`, not `pom.xml`; dependency-aware compilation and Flink/Kafka/REST integration testing could not be executed in this environment.
