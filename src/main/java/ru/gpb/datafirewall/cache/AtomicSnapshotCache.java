package ru.gpb.datafirewall.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AtomicSnapshotCache<K, V> {

    private final AtomicReference<State<K, V>> ref =
            new AtomicReference<>(State.empty());

    public V get(K key) {
        return ref.get().values().get(key);
    }

    public Map<K, V> snapshot() {
        return ref.get().values();
    }

    public String version() {
        return ref.get().version();
    }

    public boolean hasVersion() {
        return ref.get().version() != null;
    }

    public void replaceAll(Map<K, V> newValues, String newVersion) {
        Objects.requireNonNull(newValues, "newValues must not be null");
        Objects.requireNonNull(newVersion, "newVersion must not be null");

        Map<K, V> frozen = freeze(newValues);
        ref.set(new State<>(frozen, newVersion));
    }

    public void clear() {
        ref.set(State.empty());
    }

    public boolean isEmpty() {
        return ref.get().values().isEmpty();
    }

    public int size() {
        return ref.get().values().size();
    }

    /**
     * Создает immutable snapshot.
     *
     * <p>По умолчанию защищает только верхний уровень Map.
     * Для вложенных структур переопределяется в наследниках.</p>
     */
    protected Map<K, V> freeze(Map<K, V> source) {
        if (source.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    private record State<K, V>(Map<K, V> values, String version) {
        private static <K, V> State<K, V> empty() {
            return new State<>(Map.of(), null);
        }
    }
}