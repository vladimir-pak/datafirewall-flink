package com.gpb.datafirewall.bundle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/** Детерминированный бинарный формат, не зависящий от сериализации Kryo/Java. */
public final class CacheBundleStateSerializer extends TypeSerializer<CacheBundleState> {
    private static final long serialVersionUID = 1L;
    private static final int MAGIC = 0x43424E44; // CBND
    private static final int WIRE_VERSION = 1;
    private static final int MAX_LENGTH = 256 * 1024 * 1024;
    private static final int MAX_COLLECTION = 1_000_000;

    public static byte[] encode(CacheBundleState bundle) {
        bundle.verify();
        try {
            byte[] payload = payload(bundle);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(payload.length + 100);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(WIRE_VERSION);
            out.writeInt(payload.length);
            out.write(payload);
            writeString(out, bundle.checksum());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot encode CacheBundleState", e);
        }
    }

    public static CacheBundleState decode(byte[] bytes) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != MAGIC) {
                throw new IOException("Invalid cache bundle magic");
            }

            int version = in.readInt();
            if (version != WIRE_VERSION) {
                throw new IOException("Unsupported wire version " + version);
            }

            int payloadLen = checkedLength(in.readInt());
            byte[] payload = new byte[payloadLen];
            in.readFully(payload);
            String checksum = readString(in);
            if (in.available() != 0) {
                throw new IOException("Trailing bundle data");
            }

            DataInputStream p = new DataInputStream(new ByteArrayInputStream(payload));
            int schemaVersion = p.readInt();
            long epoch = p.readLong();
            String compiled = readString(p);
            String politics = readString(p);
            String handler = readString(p);
            Map<String, byte[]> bytecodes = readBytecodes(p);
            Map<String, String> dataset = readStringMap(p);
            Map<String, Map<String, Set<String>>> rules = readNestedSets(p);
            Map<String, Map<String, String>> errors = readNestedStrings(p);
            Map<String, Set<String>> excluded = readSets(p);
            Map<String, Boolean> flags = readBooleans(p);
            Map<String, String> genders = readStringMap(p);
            Map<String, String> duls = readStringMap(p);
            if (p.available() != 0) {
                throw new IOException("Trailing payload data");
            }

            CacheBundleState bundle = new CacheBundleState(
                    schemaVersion, epoch, compiled, politics, bytecodes, dataset, rules, errors,
                    excluded, flags, genders, duls, handler, checksum);
            bundle.verify();
            return bundle;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot decode CacheBundleState", e);
        }
    }

    public static String computeChecksum(CacheBundleState state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(payload(state)));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Cannot calculate cache bundle checksum", e);
        }
    }

    private static byte[] payload(CacheBundleState b) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(b.schemaVersion());
        out.writeLong(b.cacheEpoch());
        writeString(out, b.compiledRulesVersion());
        writeString(out, b.politicsVersion());
        writeString(out, b.handler());

        Map<String, byte[]> codes = new TreeMap<>(b.classNameToBytecode());
        out.writeInt(codes.size());
        for (var e : codes.entrySet()) {
            writeString(out, e.getKey());
            out.writeInt(e.getValue().length);
            out.write(e.getValue());
        }

        writeStringMap(out, b.dataset2ControlArea());
        writeNestedSets(out, b.controlAreaRules());
        writeNestedStrings(out, b.errorMessages());
        writeSets(out, b.datasetExclusion());
        writeBooleans(out, b.filterFlags());
        writeStringMap(out, b.genderMapping());
        writeStringMap(out, b.typeDulMapping());
        out.flush();
        if (bytes.size() > MAX_LENGTH) {
            throw new IOException("Cache bundle exceeds " + MAX_LENGTH + " bytes");
        }
        return bytes.toByteArray();
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] b = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int n = checkedLength(in.readInt());
        byte[] b = new byte[n];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static int checkedLength(int n) throws IOException {
        if (n < 0 || n > MAX_LENGTH) {
            throw new IOException("Invalid byte length " + n);
        }
        return n;
    }

    private static int count(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > MAX_COLLECTION) {
            throw new IOException("Invalid map size " + n);
        }
        return n;
    }

    private static Map<String, byte[]> readBytecodes(DataInputStream in) throws IOException {
        int n = count(in);
        Map<String, byte[]> m = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            String key = readString(in);
            int len = checkedLength(in.readInt());
            byte[] b = new byte[len];
            in.readFully(b);
            putUnique(m, key, b);
        }
        return m;
    }

    private static void writeStringMap(DataOutputStream out, Map<String, String> m)
            throws IOException {
        out.writeInt(m.size());
        for (var e : new TreeMap<>(m).entrySet()) {
            writeString(out, e.getKey());
            writeString(out, e.getValue());
        }
    }

    private static Map<String, String> readStringMap(DataInputStream in) throws IOException {
        int n = count(in);
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            putUnique(m, readString(in), readString(in));
        }
        return m;
    }

    private static void writeSets(DataOutputStream out, Map<String, Set<String>> m)
            throws IOException {
        out.writeInt(m.size());
        for (var e : new TreeMap<>(m).entrySet()) {
            writeString(out, e.getKey());
            TreeSet<String> sorted = new TreeSet<>(e.getValue());
            out.writeInt(sorted.size());
            for (String s : sorted) {
                writeString(out, s);
            }
        }
    }

    private static Map<String, Set<String>> readSets(DataInputStream in) throws IOException {
        int n = count(in);
        Map<String, Set<String>> m = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            String key = readString(in);
            int size = count(in);
            Set<String> s = new LinkedHashSet<>();
            for (int j = 0; j < size; j++) {
                if (!s.add(readString(in))) {
                    throw new IOException("Duplicate set item");
                }
            }
            putUnique(m, key, s);
        }
        return m;
    }

    private static void writeNestedSets(
            DataOutputStream out, Map<String, Map<String, Set<String>>> m) throws IOException {
        out.writeInt(m.size());
        for (var e : new TreeMap<>(m).entrySet()) {
            writeString(out, e.getKey());
            writeSets(out, e.getValue());
        }
    }

    private static Map<String, Map<String, Set<String>>> readNestedSets(DataInputStream in)
            throws IOException {
        int n = count(in);
        Map<String, Map<String, Set<String>>> m = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            putUnique(m, readString(in), readSets(in));
        }
        return m;
    }

    private static void writeNestedStrings(
            DataOutputStream out, Map<String, Map<String, String>> m) throws IOException {
        out.writeInt(m.size());
        for (var e : new TreeMap<>(m).entrySet()) {
            writeString(out, e.getKey());
            writeStringMap(out, e.getValue());
        }
    }

    private static Map<String, Map<String, String>> readNestedStrings(DataInputStream in)
            throws IOException {
        int n = count(in);
        Map<String, Map<String, String>> m = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            putUnique(m, readString(in), readStringMap(in));
        }
        return m;
    }

    private static void writeBooleans(DataOutputStream out, Map<String, Boolean> m)
            throws IOException {
        out.writeInt(m.size());
        for (var e : new TreeMap<>(m).entrySet()) {
            writeString(out, e.getKey());
            out.writeBoolean(e.getValue());
        }
    }

    private static Map<String, Boolean> readBooleans(DataInputStream in) throws IOException {
        int n = count(in);
        Map<String, Boolean> m = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            putUnique(m, readString(in), in.readBoolean());
        }
        return m;
    }

    private static <V> void putUnique(Map<String, V> map, String key, V val) throws IOException {
        if (map.putIfAbsent(key, val) != null) {
            throw new IOException("Duplicate key " + key);
        }
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<CacheBundleState> duplicate() {
        return this;
    }

    @Override
    public CacheBundleState createInstance() {
        return null;
    }

    @Override
    public CacheBundleState copy(CacheBundleState from) {
        return from;
    }

    @Override
    public CacheBundleState copy(CacheBundleState from, CacheBundleState reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(CacheBundleState value, DataOutputView target) throws IOException {
        byte[] b = encode(value);
        target.writeInt(b.length);
        target.write(b);
    }

    @Override
    public CacheBundleState deserialize(DataInputView source) throws IOException {
        int n = checkedLength(source.readInt());
        byte[] b = new byte[n];
        source.readFully(b);
        return decode(b);
    }

    @Override
    public CacheBundleState deserialize(CacheBundleState reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int n = checkedLength(source.readInt());
        target.writeInt(n);
        byte[] buffer = new byte[8192];
        while (n > 0) {
            int read = Math.min(n, buffer.length);
            source.readFully(buffer, 0, read);
            target.write(buffer, 0, read);
            n -= read;
        }
    }

    @Override
    public TypeSerializerSnapshot<CacheBundleState> snapshotConfiguration() {
        return new Snapshot();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof CacheBundleStateSerializer;
    }

    @Override
    public int hashCode() {
        return CacheBundleStateSerializer.class.hashCode();
    }

    public static final class Snapshot extends SimpleTypeSerializerSnapshot<CacheBundleState> {
        public Snapshot() {
            super(CacheBundleStateSerializer::new);
        }
    }
}
