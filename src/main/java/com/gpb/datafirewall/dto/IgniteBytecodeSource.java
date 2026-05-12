package com.gpb.datafirewall.dto;

import java.util.Map;
import java.util.Objects;

import com.gpb.datafirewall.ignite.BytecodeSource;
import com.gpb.datafirewall.ignite.IgniteClientFacade;

/**
 * Источник байткода правил, использующий Apache Ignite в качестве хранилища.
 *
 * <p>Является адаптером над {@link IgniteClientFacade} и предоставляет унифицированный
 * интерфейс {@link BytecodeSource} для загрузки скомпилированных правил.</p>
 *
 * <p>Отвечает за валидацию входных параметров и делегирует фактическую загрузку данных
 * в инфраструктурный слой Ignite.</p>
 */
public final class IgniteBytecodeSource implements BytecodeSource {

    private final IgniteClientFacade ignite;

    public IgniteBytecodeSource(IgniteClientFacade ignite) {
        this.ignite = Objects.requireNonNull(ignite, "ignite");
    }

    @Override
    public Map<String, byte[]> loadAll(String cacheName) {
        if (cacheName == null || cacheName.isBlank()) {
            throw new IllegalArgumentException("cacheName must not be null/blank");
        }
        return ignite.loadAllBytecodes(cacheName);
    }
}
