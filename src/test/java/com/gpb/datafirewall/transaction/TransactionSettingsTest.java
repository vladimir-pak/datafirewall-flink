package com.gpb.datafirewall.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TransactionSettingsTest {

    private static TransactionSettings settings(String input, String output) {
        return new TransactionSettings(
                "mq", "localhost", 1414, "DEV.APP.SVRCONN", "QM1",
                input, output, "user", "password",
                false, null, null, null, "DFW", "service",
                null, 100);
    }

    @Test
    void allowsDistinctInputAndOutputQueues() {
        assertEquals("INPUT.Q", settings("INPUT.Q", "OUTPUT.Q").inputQueue());
        assertEquals("OUTPUT.Q", settings("INPUT.Q", "OUTPUT.Q").outputQueue());
    }

    @Test
    void rejectsMissingInputQueue() {
        assertThrows(IllegalArgumentException.class,
                () -> settings("", "OUTPUT.Q"));
    }

    @Test
    void rejectsMissingOutputQueue() {
        assertThrows(IllegalArgumentException.class,
                () -> settings("INPUT.Q", ""));
    }

    @Test
    void rejectsInputOutputOverlap() {
        assertThrows(IllegalArgumentException.class,
                () -> settings("INPUT.Q", "INPUT.Q"));
    }
}
