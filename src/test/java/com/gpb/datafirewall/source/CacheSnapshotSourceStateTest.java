package com.gpb.datafirewall.source;

import com.gpb.datafirewall.bundle.CacheBundleState;
import com.gpb.datafirewall.bundle.CacheBundleStateSerializer;
import com.gpb.datafirewall.testfixtures.TestBundles;
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
        out.writeInt(0x43535331); out.writeLong(123); out.writeLong(-1); out.writeLong(-1);
        byte[] encoded = CacheBundleStateSerializer.encode(bundle); out.writeInt(encoded.length); out.write(encoded);
        var state = new BytesState(); state.add(bytes.toByteArray());
        var source = source(); source.initializeState(context(state, true));
        List<CacheBundleState> received = new ArrayList<>();
        class StopAfterRestore extends RuntimeException {}
        var ctx = new SourceFunction.SourceContext<CacheBundleState>() {
            public void collect(CacheBundleState value) { received.add(value); throw new StopAfterRestore(); }
            public void collectWithTimestamp(CacheBundleState value, long timestamp) { collect(value); }
            public void emitWatermark(Watermark mark) { }
            public void markAsTemporarilyIdle() { }
            public Object getCheckpointLock() { return this; }
            public void close() { }
        };
        assertThrows(StopAfterRestore.class, () -> source.run(ctx));
        assertEquals(42, received.get(0).cacheEpoch());
        assertEquals(bundle.checksum(), received.get(0).checksum());
        source.snapshotState(null); // same persisted state, including exact Kafka offset
        assertArrayEquals(bytes.toByteArray(), state.values.get(0));
    }
    @Test void freshDeserializedSourceStartsWithUnknownOffset() throws Exception {
        var bytes = new ByteArrayOutputStream();
        new ObjectOutputStream(bytes).writeObject(source());
        var source = (CacheSnapshotSource) new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())).readObject();
        var state = new BytesState(); source.initializeState(context(state, false)); source.snapshotState(null);
        var in = new DataInputStream(new ByteArrayInputStream(state.values.get(0)));
        assertEquals(0x43535331, in.readInt());
        assertEquals(-1, in.readLong()); assertEquals(-1, in.readLong()); assertEquals(-1, in.readLong());
        assertEquals(0, in.readInt());
    }
}
