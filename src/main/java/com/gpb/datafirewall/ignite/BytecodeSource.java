package com.gpb.datafirewall.ignite;

import java.util.Map;

public interface BytecodeSource {
    /** @return className -> bytecode */
    Map<String, byte[]> loadAll(String sourceName);
}

