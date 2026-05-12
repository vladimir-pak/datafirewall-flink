package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import ru.gpb.datafirewall.services.JsonEventProcessor;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class JsonEventProcessorCasesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    record Case(String inputResource, String expectedResource) {}

    static Stream<Case> cases() {
        return Stream.of(
                new Case("input/case1_normal.jsonl",         "input/case1_expected.json"),
                new Case("input/case2_null_mapping.jsonl",   "input/case2_expected.json"),
                new Case("input/case3_null_value.jsonl",     "input/case3_expected.json"),
                new Case("input/case4_none_mapping.jsonl",   "input/case4_expected.json"),
                new Case("input/case5_missing_fields.jsonl", "input/case5_expected.json")
        );
    }

    @ParameterizedTest
    @MethodSource("cases")
    void should_convert_cases_correctly(Case c) throws Exception {
        String inputLine = readResource(c.inputResource).trim();
        String expectedJson = readResource(c.expectedResource).trim();

        JsonEventProcessor processor = new JsonEventProcessor(new ObjectMapper());

        String actual = processor.toFlatJson(inputLine).orElseThrow();

        // сравниваем как JSON-деревья (порядок полей не важен)
        JsonNode actualNode = MAPPER.readTree(actual);
        JsonNode expectedNode = MAPPER.readTree(expectedJson);

        assertThat(actualNode).isEqualTo(expectedNode);
    }

    private static String readResource(String classpathPath) throws Exception {
        try (InputStream is = JsonEventProcessorCasesTest.class
                .getClassLoader()
                .getResourceAsStream(classpathPath)) {

            if (is == null) {
                throw new IllegalStateException(
                        "Resource not found on classpath: " + classpathPath +
                                "\nExpected under: src/test/resources/" + classpathPath
                );
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
