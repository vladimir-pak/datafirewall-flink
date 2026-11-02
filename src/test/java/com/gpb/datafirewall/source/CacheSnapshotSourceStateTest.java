package com.gpb.datafirewall.source;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheBundleEnvelope;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.testfixtures.TestBundles;
import com.gpb.datafirewall.kafka.CacheUpdateEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.lang.reflect.Proxy;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("deprecation")
class CacheSnapshotSourceStateTest {
    private static final class BytesState implements ListState<byte[]> {
        List<byte[]> values = new ArrayList<>();
        public Iterable<byte[]> get() { return values; }
        public void clear() { values.clear(); }
        public void add(byte[] value) { values.add(value); }
        public void update(List<byte[]> value) { values = new ArrayList<>(value); }
        public void addAll(List<byte[]> value) { values.addAll(value); }
    }
    private static FunctionInitializationContext context(BytesState state, boolean restored) {
        var store = (OperatorStateStore) Proxy.newProxyInstance(CacheSnapshotSourceStateTest.class.getClassLoader(),
                new Class<?>[]{OperatorStateStore.class}, (p,m,a) -> state);
        return (FunctionInitializationContext) Proxy.newProxyInstance(CacheSnapshotSourceStateTest.class.getClassLoader(),
                new Class<?>[]{FunctionInitializationContext.class}, (p,m,a) -> switch (m.getName()) {
                    case "isRestored" -> restored;
                    case "getRestoredCheckpointId" -> restored ? OptionalLong.of(10) : OptionalLong.empty();
                    case "getOperatorStateStore" -> store;
                    default -> null;
                });
    }
    private static CacheSnapshotSource source() {
        return new CacheSnapshotSource(ParameterTool.fromMap(Map.of("cache.bootstrap.enabled", "false",
                "ignite.apiUrl", "http://127.0.0.1:1")), null, new Properties(),
                "127.0.0.1:1", "cache-updates", "test");
    }
    @Test void restoresRealSourceBundleBeforeKafkaOrApiAccessAndKeepsWireFormat() throws Exception {
        var bundle = TestBundles.bundle(42);
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeInt(0x43535332); out.writeLong(123);
        byte[] encoded = CacheBundleStateSerializer.encode(bundle); out.writeInt(encoded.length); out.write(encoded); out.writeInt(0);
        var state = new BytesState(); state.add(bytes.toByteArray());
        var source = source(); source.initializeState(context(state, true));
        List<CacheBundleEnvelope> received = new ArrayList<>();
        class StopAfterRestore extends RuntimeException {}
        var ctx = new SourceFunction.SourceContext<CacheBundleEnvelope>() {
            public void collect(CacheBundleEnvelope value) { received.add(value); throw new StopAfterRestore(); }
            public void collectWithTimestamp(CacheBundleEnvelope value, long timestamp) { collect(value); }
            public void emitWatermark(Watermark mark) { }
            public void markAsTemporarilyIdle() { }
            public Object getCheckpointLock() { return this; }
            public void close() { }
        };
        assertThrows(StopAfterRestore.class, () -> source.run(ctx));
        assertEquals("42", received.get(0).rulesVersion);
        assertEquals(bundle.checksum(), received.get(0).checksum);
        assertFalse(received.get(0).handlerOnly);
        source.snapshotState(null); // full state, Kafka offset and new routing field
        try (var saved = new DataInputStream(new ByteArrayInputStream(state.values.get(0)))) {
            assertEquals(0x43535332, saved.readInt());
            assertEquals(123, saved.readLong());
            int length = saved.readInt();
            assertEquals(encoded.length, length);
            assertArrayEquals(encoded, saved.readNBytes(length));
            assertEquals(0, saved.readInt());
            assertEquals(bundle.handler(), saved.readUTF());
            assertFalse(saved.readBoolean());
            assertEquals(0, saved.available());
        }
    }
    @Test void restoresStagedCandidateWithoutActivatingIt() throws Exception {
        var active = TestBundles.bundle(42);
        var staged = TestBundles.bundle(43);
        byte[] encodedActive = CacheBundleStateSerializer.encode(active);
        byte[] encodedStaged = CacheBundleStateSerializer.encode(staged);
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x43535332);
        out.writeLong(500);
        out.writeInt(encodedActive.length);
        out.write(encodedActive);
        out.writeInt(encodedStaged.length);
        out.write(encodedStaged);
        var state = new BytesState();
        state.add(bytes.toByteArray());
        var source = source();
        source.initializeState(context(state, true));
        source.snapshotState(null);
        try (var saved = new DataInputStream(new ByteArrayInputStream(state.values.get(0)))) {
            assertEquals(0x43535332, saved.readInt());
            assertEquals(500, saved.readLong());
            int activeLen = saved.readInt();
            assertArrayEquals(encodedActive, saved.readNBytes(activeLen));
            int stagedLen = saved.readInt();
            assertArrayEquals(encodedStaged, saved.readNBytes(stagedLen));
            assertEquals(active.handler(), saved.readUTF());
            assertFalse(saved.readBoolean());
        }
    }

    @Test void restoredHandlerChangeIsRebroadcastWithoutReloadingCache() throws Exception {
        var bundle = TestBundles.bundle(42);
        byte[] encoded = CacheBundleStateSerializer.encode(bundle);
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x43535332);
        out.writeLong(123);
        out.writeInt(encoded.length);
        out.write(encoded);
        out.writeInt(0);
        out.writeUTF("dotnet");
        out.writeBoolean(true);
        var state = new BytesState();
        state.add(bytes.toByteArray());
        var source = source();
        source.initializeState(context(state, true));
        List<CacheBundleEnvelope> received = new ArrayList<>();
        class StopAfterHandler extends RuntimeException { }
        var ctx = new SourceFunction.SourceContext<CacheBundleEnvelope>() {
            public void collect(CacheBundleEnvelope value) {
                received.add(value);
                if (received.size() == 2) throw new StopAfterHandler();
            }
            public void collectWithTimestamp(CacheBundleEnvelope value, long timestamp) { collect(value); }
            public void emitWatermark(Watermark mark) { }
            public void markAsTemporarilyIdle() { }
            public Object getCheckpointLock() { return this; }
            public void close() { }
        };
        assertThrows(StopAfterHandler.class, () -> source.run(ctx));
        assertEquals(2, received.size());
        assertFalse(received.get(0).handlerOnly);
        assertTrue(received.get(1).handlerOnly);
        assertEquals("dotnet", received.get(1).handler);
        assertNull(received.get(1).bytes);
        source.snapshotState(null);
        assertArrayEquals(bytes.toByteArray(), state.values.get(0));
    }

    @Test void handlerOnlyEmitsTinyEventAndKeepsStagedBundleUntouched() throws Exception {
        var active = TestBundles.bundle(42);
        var staged = TestBundles.bundle(43);
        byte[] activeBytes = CacheBundleStateSerializer.encode(active);
        byte[] stagedBytes = CacheBundleStateSerializer.encode(staged);
        var data = new ByteArrayOutputStream();
        var out = new DataOutputStream(data);
        out.writeInt(0x43535332);
        out.writeLong(600);
        out.writeInt(activeBytes.length);
        out.write(activeBytes);
        out.writeInt(stagedBytes.length);
        out.write(stagedBytes);
        var state = new BytesState();
        state.add(data.toByteArray());
        var source = source();
        source.initializeState(context(state, true));
        List<CacheBundleEnvelope> emitted = new ArrayList<>();
        var ctx = new SourceFunction.SourceContext<CacheBundleEnvelope>() {
            public void collect(CacheBundleEnvelope value) { emitted.add(value); }
            public void collectWithTimestamp(CacheBundleEnvelope value, long timestamp) { collect(value); }
            public void emitWatermark(Watermark mark) { }
            public void markAsTemporarilyIdle() { }
            public Object getCheckpointLock() { return this; }
            public void close() { }
        };
        source.applyHandlerOnly(ctx, new CacheUpdateEvent(0, "handler", "dotnet"), 601);
        assertEquals(1, emitted.size());
        assertTrue(emitted.get(0).handlerOnly);
        assertNull(emitted.get(0).bytes);
        source.snapshotState(null);
        try (var saved = new DataInputStream(new ByteArrayInputStream(state.values.get(0)))) {
            assertEquals(0x43535332, saved.readInt());
            assertEquals(601, saved.readLong());
            assertArrayEquals(activeBytes, saved.readNBytes(saved.readInt()));
            assertArrayEquals(stagedBytes, saved.readNBytes(saved.readInt()));
            assertEquals("dotnet", saved.readUTF());
            assertTrue(saved.readBoolean());
            assertEquals(0, saved.available());
        }
    }

    @Test void freshDeserializedSourceStartsWithUnknownOffset() throws Exception {
        var bytes = new ByteArrayOutputStream();
        new ObjectOutputStream(bytes).writeObject(source());
        var source = (CacheSnapshotSource) new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())).readObject();
        var state = new BytesState(); source.initializeState(context(state, false)); source.snapshotState(null);
        var in = new DataInputStream(new ByteArrayInputStream(state.values.get(0)));
        assertEquals(0x43535332, in.readInt());
        assertEquals(-1, in.readLong());
        assertEquals(0, in.readInt());
    }
}
