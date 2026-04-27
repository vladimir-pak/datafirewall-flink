package ru.gpbapp.datafirewallflink.services;

import java.io.Serializable;
import java.util.Arrays;

public final class MessageReply implements Serializable {

    public static final int MQ_ID_LEN = 24;

    public byte[] correlId;
    public String payload;

    public MessageReply() {}

    public MessageReply(byte[] correlId, String payload) {
        this.correlId = normalizeId(correlId);
        this.payload = payload;
    }

    private static byte[] normalizeId(byte[] id) {
        if (id == null) return null;

        if (id.length == MQ_ID_LEN) {
            return Arrays.copyOf(id, MQ_ID_LEN);
        }

        byte[] out = new byte[MQ_ID_LEN];
        System.arraycopy(id, 0, out, 0, Math.min(id.length, MQ_ID_LEN));
        return out;
    }

    @Override
    public String toString() {
        return "MessageReply{" +
                "correlId=" + (correlId == null ? "null" : ("byte[" + correlId.length + "]")) +
                ", payloadLen=" + (payload == null ? 0 : payload.length()) +
                '}';
    }
}