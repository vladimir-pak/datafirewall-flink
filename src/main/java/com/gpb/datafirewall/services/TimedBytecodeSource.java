package com.gpb.datafirewall.services;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import com.gpb.datafirewall.ignite.BytecodeSource;

public final class TimedBytecodeSource implements BytecodeSource {
    private final BytecodeSource delegate;
    private final Consumer<String> log;

    public TimedBytecodeSource(BytecodeSource delegate, Consumer<String> log) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.log = (log != null) ? log : System.out::println;
    }

    @Override
    public Map<String, byte[]> loadAll(String name) {
        long t0 = System.nanoTime();
        try {
            Map<String, byte[]> res = delegate.loadAll(name);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            int size = (res == null) ? -1 : res.size();
            long bytes = totalBytes(res);
            log.accept("[RULES] loadAll('" + name + "') -> classes=" + size + ", bytes=" + bytes + ", took=" + ms + "ms");
            return res;
        } catch (RuntimeException e) {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.accept("[RULES] loadAll('" + name + "') FAILED after " + ms + "ms: " + e.getMessage());
            throw e;
        }
    }

    private static long totalBytes(Map<String, byte[]> m) {
        if (m == null) return 0;
        long sum = 0;
        for (byte[] b : m.values()) {
            if (b != null) sum += b.length;
        }
        return sum;
    }
}
