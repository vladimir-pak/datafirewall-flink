package com.gpb.datafirewall.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.converter.EventToFlatProfile;
import com.gpb.datafirewall.dto.FlatProfileDto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Обрабатывает входящие JSON-события:
 * <ul>
 *     <li>проверяет корректность входной строки</li>
 *     <li>парсит JSON в дерево</li>
 *     <li>преобразует событие в «плоский» профиль через {@link EventToFlatProfile}</li>
 *     <li>сериализует результат обратно в JSON-строку</li>
 * </ul>
 *
 * <p>Класс устойчив к ошибкам: некорректные, пустые и проблемные сообщения
 * безопасно отбрасываются без остановки пайплайна обработки.</p>
 */
public class JsonEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(JsonEventProcessor.class);

    private final ObjectMapper mapper;
    private final EventToFlatProfile converter;

    public JsonEventProcessor(ObjectMapper mapper) {
        this.mapper = mapper;
        this.converter = new EventToFlatProfile(mapper);
    }

    /** @return Optional.empty() если строка пустая/битая */
    public Optional<String> toFlatJson(String jsonLine) {
        return toFlatProfile(jsonLine).flatMap(profile -> {
            try {
                return Optional.of(mapper.writeValueAsString(profile.json()));
            } catch (Exception e) {
                log.warn("Skip line due to serialization error: {}", safeSnippet(safeTrim(jsonLine)), e);
                return Optional.empty();
            }
        });
    }

    /** @return Optional.empty() если строка пустая/битая */
    public Optional<FlatProfileDto> toFlatProfile(String jsonLine) {
        if (jsonLine == null) return Optional.empty();
        String s = safeTrim(jsonLine);
        if (s.isEmpty()) return Optional.empty();

        try {
            JsonNode event = mapper.readTree(s);
            return toFlatProfile(event);

        } catch (JsonProcessingException e) {
            log.debug("Skip invalid json line: {}", safeSnippet(s), e);
            return Optional.empty();

        } catch (Exception e) {
            log.warn("Skip line due to processing error: {}", safeSnippet(s), e);
            return Optional.empty();
        }
    }

    public Optional<FlatProfileDto> toFlatProfile(JsonNode event) {
        if (event == null) return Optional.empty();
        try {
            FlatProfileDto profile = converter.convertToProfile(event);
            return Optional.of(profile);
        } catch (Exception e) {
            log.warn("Skip event due to processing error", e);
            return Optional.empty();
        }
    }

    private static String safeTrim(String s) {
        return s == null ? "" : s.trim();
    }

    private static String safeSnippet(String s) {
        int max = 200;
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
