package com.gpb.datafirewall.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface Rule extends Serializable {

    boolean apply(Map<String, String> data);

    default Set<String> referencedFields() {
        return Set.of();
    }
}
