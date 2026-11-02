package com.gpb.datafirewall.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TransactionSettingsTest {

    private static TransactionSettings settings(String input, String output, String error) {
        return new TransactionSettings(
                "mq", "localhost", 1414, "DEV.APP.SVRCONN", "QM1",
                input, output, error, "user", "password",
                false, null, null, null, "DFW", "service",
                null, 100);
    }

    @Test
    void allowsDistinctQueues() {
        assertEquals("ERROR.Q", settings("INPUT.Q", "OUTPUT.Q", "ERROR.Q").errorQueue());
    }

    @Test
    void rejectsMissingErrorQueue() {
        assertThrows(IllegalArgumentException.class,
                () -> settings("INPUT.Q", "OUTPUT.Q", ""));
    }

    @Test
    void rejectsInputOutputOverlap() {
        assertThrows(IllegalArgumentException.class,
                () -> settings("INPUT.Q", "INPUT.Q", "ERROR.Q"));
    }

    @Test
    void rejectsErrorOutputOverlap() {
        assertThrows(IllegalArgumentException.class,
                () -> settings("INPUT.Q", "ERROR.Q", "ERROR.Q"));
    }
}
