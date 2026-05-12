package com.gpb.datafirewall.ignite.impl;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

import com.gpb.datafirewall.ignite.IgniteClientFacade;

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;

import javax.cache.Cache;
import java.util.HashMap;
import java.util.Map;

/**
 * Реализация {@link IgniteClientFacade} на базе Apache Ignite Thin Client.
 *
 * <p>Отвечает за подключение к Ignite-кластеру и загрузку байткода правил
 * из указанного кэша.</p>
 *
 * <p>Загружает все записи из кэша вида {@code имя_класса → bytecode}
 * и возвращает их в виде {@link Map} для дальнейшей динамической загрузки правил.</p>
 *
 * <p>Корректно управляет жизненным циклом соединения с Ignite
 * и поддерживает безопасное освобождение ресурсов.</p>
 */
public final class IgniteClientFacadeImpl implements IgniteClientFacade, AutoCloseable {

    private final IgniteClient client;

    public IgniteClientFacadeImpl(String host, int port) {
        ClientConfiguration cfg = new ClientConfiguration()
                .setAddresses(host + ":" + port);
        this.client = Ignition.startClient(cfg);
    }

    @Override
    public Map<String, byte[]> loadAllBytecodes(String cacheName) {
        ClientCache<String, byte[]> cache = client.cache(cacheName);
        if (cache == null) {
            throw new IllegalStateException("Ignite cache not found: " + cacheName);
        }

        Map<String, byte[]> result = new HashMap<>();

        try (QueryCursor<Cache.Entry<String, byte[]>> cursor = cache.query(new ScanQuery<>())) {
            for (Cache.Entry<String, byte[]> e : cursor) {
                String key = e.getKey();
                byte[] val = e.getValue();
                if (key != null && val != null) {
                    result.put(key, val);
                }
            }
        }

        return result;
    }


    @Override
    public void close() {
        client.close();
    }
}
