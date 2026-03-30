package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

public record ValidationResult(
        ObjectNode details,
        String allResult,
        String processStatus,
        Map<String, Map<String, String>> detailByField,
        Map<String, Map<String, Map<String, String>>> detailByDataset
) {}