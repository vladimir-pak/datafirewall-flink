package ru.gpb.datafirewall.ignite;

import java.util.Map;

public interface IgniteClientFacade {
    Map<String, byte[]> loadAllBytecodes(String cacheName);
}
