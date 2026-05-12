package ru.gpb.datafirewall.dto;

import java.io.Serializable;
import java.util.Map;

public interface Rule extends Serializable {
    boolean apply(Map<String, String> data);
}
