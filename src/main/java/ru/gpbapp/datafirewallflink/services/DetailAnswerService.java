package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.validation.DetailAnswerBuilder;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

public final class DetailAnswerService {

    private static final Logger log = LoggerFactory.getLogger(DetailAnswerService.class);

    private final ObjectMapper mapper;
    private final DetailAnswerBuilder builder;

    public DetailAnswerService(ObjectMapper mapper) {
        this.mapper = mapper;
        this.builder = new DetailAnswerBuilder(mapper);
    }

    public String build(JsonNode originalEvent, ValidationResult validation) {
        if (originalEvent == null || validation == null) return null;

        try {
            ObjectNode node = builder.buildDetailAnswer(originalEvent, validation);
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            log.warn("Failed to build detail answer", e);
            return null;
        }
    }
}