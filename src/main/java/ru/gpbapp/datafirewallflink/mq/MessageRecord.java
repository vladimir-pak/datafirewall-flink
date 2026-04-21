package ru.gpbapp.datafirewallflink.mq;

import java.io.Serializable;
import java.util.Arrays;

public final class MessageRecord implements Serializable {

    public static final int MQ_ID_LEN = 24;

    public byte[] msgId;
    public String payload;

    public MessageRecord() {}

    public MessageRecord(byte[] msgId, String payload) {
        this.msgId = normalizeId(msgId);
        this.payload = payload;
    }

    private static byte[] normalizeId(byte[] id) {
        if (id == null) return null;

        // важно: MQ требует 24 байта, Artemis — нет, но делаем унификацию
        if (id.length == MQ_ID_LEN) {
            return Arrays.copyOf(id, MQ_ID_LEN);
        }

        byte[] out = new byte[MQ_ID_LEN];
        System.arraycopy(id, 0, out, 0, Math.min(id.length, MQ_ID_LEN));
        return out;
    }

    @Override
    public String toString() {
        return "MessageRecord{" +
                "msgId=" + (msgId == null ? "null" : ("byte[" + msgId.length + "]")) +
                ", payloadLen=" + (payload == null ? 0 : payload.length()) +
                '}';
    }
}