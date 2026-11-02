package com.gpb.datafirewall.coordination;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.function.Consumer;

public interface CacheActivationEvent extends OperatorEvent {

    record Hello() implements CacheActivationEvent {}
    record Chunk(long epoch, String checksum, int length, int offset, byte[] data)
            implements CacheActivationEvent {}
    record Ready(long epoch, String checksum) implements CacheActivationEvent {}
    record Pause(long epoch, String checksum) implements CacheActivationEvent {}
    record Paused(long epoch, String checksum) implements CacheActivationEvent {}
    record Activate(long epoch, String checksum) implements CacheActivationEvent {}
    record Activated(long epoch, String checksum) implements CacheActivationEvent {}
    record Resume(long epoch, String checksum) implements CacheActivationEvent {}

    int CHUNK_SIZE = 256 * 1024;
    int MAX_BUNDLE_BYTES = 256 * 1024 * 1024;

    static void sendBundle(
            long epoch, 
            String checksum, 
            byte[] bytes, 
            Consumer<OperatorEvent> send) {
        if (bytes.length == 0 || bytes.length > MAX_BUNDLE_BYTES)
            throw new IllegalArgumentException(
                    "Invalid bundle length: " + bytes.length
        );
        for (int offset = 0; offset < bytes.length; offset += CHUNK_SIZE) {
            send.accept(new Chunk(epoch, checksum, bytes.length, offset,
                    Arrays.copyOfRange(bytes, offset, Math.min(bytes.length, offset + CHUNK_SIZE))));
        }
    }

    final class Assembler {
        private ByteArrayOutputStream buffer;
        private long epoch;
        private String checksum;
        private int length;

        public byte[] accept(Chunk part) {
            if (part.length() <= 0 || part.length() > MAX_BUNDLE_BYTES || part.data() == null
                    || part.data().length == 0 || part.data().length > CHUNK_SIZE)
                throw new IllegalArgumentException("Invalid cache transfer chunk");
            if (part.offset() == 0) {
                if (buffer != null) throw new IllegalStateException("Interleaved cache transfers");
                buffer = new ByteArrayOutputStream(Math.min(part.length(), CHUNK_SIZE));
                epoch = part.epoch(); checksum = part.checksum(); length = part.length();
            }
            if (buffer == null || epoch != part.epoch() || !checksum.equals(part.checksum())
                    || length != part.length() || part.offset() != buffer.size()
                    || part.data().length > length - buffer.size())
                throw new IllegalStateException("Out-of-order cache transfer");
            buffer.write(part.data(), 0, part.data().length);
            if (buffer.size() != length) return null;
            byte[] result = buffer.toByteArray();
            buffer = null;
            return result;
        }
    }
}
