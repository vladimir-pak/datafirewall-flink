package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.validation.DetailAnswerBuilder;
import com.gpb.datafirewall.validation.ValidationResult;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DetailAnswerService {

    private static final Logger log = LoggerFactory.getLogger(DetailAnswerService.class);

    private final ObjectMapper mapper;
    private final DetailAnswerBuilder builder;

    public DetailAnswerService(ObjectMapper mapper) {
        this.mapper = mapper;
        this.builder = new DetailAnswerBuilder(mapper);
    }

    public String build(
        JsonNode originalEvent, 
        ValidationResult validation,
        String qid,
        Set<String> excludedBlocks,
        String controlArea
    ) {
        if (originalEvent == null || validation == null)
            throw new IllegalArgumentException("Original event and validation are required");

        try {
            ObjectNode node = builder.buildDetailAnswer(
                originalEvent,
                validation,
                qid,
                excludedBlocks,
                controlArea
            );
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build detail answer", e);
        }
    }
}