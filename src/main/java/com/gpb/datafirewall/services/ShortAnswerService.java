package com.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.validation.AnswerBuilder;
import com.gpb.datafirewall.validation.ValidationResult;
import com.gpb.datafirewall.validation.ValidationMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShortAnswerService {

    private static final Logger log = LoggerFactory.getLogger(ShortAnswerService.class);

    private final ObjectMapper mapper;
    private final AnswerBuilder answerBuilder;

    public ShortAnswerService(ObjectMapper mapper) {
        this.mapper = mapper;
        this.answerBuilder = new AnswerBuilder(mapper);
    }

    public String build(
        JsonNode originalEvent, 
        ValidationResult validation, 
        String qid,
        String controlArea
    ) {
        return build(originalEvent, validation, qid, controlArea, null);
    }

    public String build(
        JsonNode originalEvent,
        ValidationResult validation,
        String qid,
        String controlArea,
        ValidationMessages classifiedMessages
    ) {
        if (originalEvent == null || validation == null)
            throw new IllegalArgumentException("Original event and validation are required");

        try {
            ObjectNode node = answerBuilder.buildAnswer(
                originalEvent, 
                validation,
                qid,
                controlArea,
                classifiedMessages
            );
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build short ANSWER", e);
        }
    }
}