package ru.gpb.datafirewall.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ru.gpb.datafirewall.validation.AnswerBuilder;
import ru.gpb.datafirewall.validation.ValidationResult;

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

    public String build(JsonNode originalEvent, ValidationResult validation) {
        if (originalEvent == null || validation == null) return null;

        try {
            ObjectNode node = answerBuilder.buildAnswer(originalEvent, validation);
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            log.warn("Failed to build short ANSWER json", e);
            return null;
        }
    }
}