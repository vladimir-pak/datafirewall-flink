package com.gpb.datafirewall.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HexUtilsTest {

    @Test
    void convertsSignedBytesToUppercaseHex() {
        assertEquals(
                "00010F107FFF80A5",
                HexUtils.toUpperHex(new byte[]{0, 1, 15, 16, 127, -1, -128, (byte) 0xA5})
        );
    }

    @Test
    void preservesNullAndEmptySemanticsForCallers() {
        assertNull(HexUtils.toUpperHex(null));
        assertEquals("", HexUtils.toUpperHex(new byte[0]));
    }
}
