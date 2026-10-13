package com.gpb.datafirewall.util;

/**
 * Быстрое преобразование бинарных идентификаторов в HEX без String.format
 * и связанных с ним временных объектов.
 */
public final class HexUtils {

    private static final char[] UPPER_HEX =
            "0123456789ABCDEF".toCharArray();

    private HexUtils() {
    }

    public static String toUpperHex(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        char[] chars = new char[bytes.length * 2];

        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xFF;
            chars[i * 2] = UPPER_HEX[value >>> 4];
            chars[i * 2 + 1] = UPPER_HEX[value & 0x0F];
        }

        return new String(chars);
    }
}
