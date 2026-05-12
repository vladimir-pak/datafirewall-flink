package com.gpb.datafirewall.rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.gpb.datafirewall.cache.CompiledRulesRegistry;
import com.gpb.datafirewall.ignite.BytecodeSource;
import com.gpb.datafirewall.model.Rule;

public final class RulesReloader {

    private final BytecodeSource source;
    private final CompiledRulesRegistry registry;

    public RulesReloader(BytecodeSource source, CompiledRulesRegistry registry) {
        this.source = Objects.requireNonNull(source, "source");
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public void reloadAllStrict(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be null/blank");
        }

        Map<String, byte[]> bytecodes = source.loadAll(name);
        if (bytecodes == null) {
            throw new IllegalStateException("BytecodeSource returned null for name=" + name);
        }

        List<String> classNames = new ArrayList<>(bytecodes.keySet());
        classNames.sort(String::compareTo);

        long totalBytes = 0;

        for (String clsName : classNames) {
            byte[] bytes = bytecodes.get(clsName);
            if (clsName == null || clsName.isBlank()) {
                throw new IllegalStateException("Found empty/null class name key in '" + name + "'");
            }
            if (bytes == null || bytes.length == 0) {
                throw new IllegalStateException(
                        "Empty bytecode for class '" + clsName + "' in '" + name + "'"
                );
            }
            totalBytes += bytes.length;
        }

        RuleClassLoader cl = new RuleClassLoader(bytecodes);
        Map<String, Rule> newRules = new HashMap<>();

        for (String clsName : classNames) {
            try {
                Class<? extends Rule> ruleClass = cl.loadRule(clsName);
                Rule rule = ruleClass.getDeclaredConstructor().newInstance();
                newRules.put(clsName, rule);
            } catch (Throwable e) {
                throw new RuntimeException(
                        "Failed to load rule '" + clsName + "' from '" + name +
                                "'. Available keys=" + classNames,
                        e
                );
            }
        }

        String version = extractVersion(name);
        registry.replaceAll(newRules, version);
    }

    private String extractVersion(String name) {
        int idx = name.lastIndexOf('_');
        if (idx < 0 || idx == name.length() - 1) {
            throw new IllegalArgumentException(
                    "Cannot extract version from cache name '" + name +
                            "'. Expected format: <cache_name>_<version>"
            );
        }

        String version = name.substring(idx + 1).trim();
        if (version.isEmpty()) {
            throw new IllegalArgumentException(
                    "Extracted empty version from cache name '" + name + "'"
            );
        }

        return version;
    }
}