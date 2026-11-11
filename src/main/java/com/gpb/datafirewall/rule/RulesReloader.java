package com.gpb.datafirewall.rule;

import com.gpb.datafirewall.model.Rule;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builds rule objects without modifying the active rules registry. */
public final class RulesReloader {
    public record PreparedRules(Map<String,Rule> rules,String version) {}

    public PreparedRules loadFromBytecodes(Map<String,byte[]> bytecodes,String version) {
        if (version == null || version.isBlank()) throw new IllegalArgumentException("Missing rules version");
        if (bytecodes == null || bytecodes.isEmpty()) throw new IllegalArgumentException("Empty bytecodes");
        for(String name:bytecodes.keySet()) {
            if(name == null || name.isBlank() || bytecodes.get(name)==null || bytecodes.get(name).length==0)
                throw new IllegalArgumentException("Missing name/bytecode for rule " + name);
        }
        List<String> classes = new ArrayList<>(bytecodes.keySet());
        classes.sort(String::compareTo);
        RuleClassLoader loader=new RuleClassLoader(bytecodes);
        Map<String,Rule> prepared=new LinkedHashMap<>();
        for(String name:classes) {
            try {
                Class<? extends Rule> cls=loader.loadRule(name);
                prepared.put(name,cls.getDeclaredConstructor().newInstance());
            } catch (ReflectiveOperationException | LinkageError | ClassCastException ex) {
                throw new IllegalStateException("Failed to prepare rule " + name + " version=" + version,ex);
            }
        }
        return new PreparedRules(Collections.unmodifiableMap(prepared),version);
    }
}
