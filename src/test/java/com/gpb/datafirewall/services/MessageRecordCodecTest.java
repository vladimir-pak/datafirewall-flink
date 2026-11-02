package com.gpb.datafirewall.services;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Test;

class MessageRecordCodecTest {

    @Test
    void mqHeadersAndTimestampsAreRetained() throws Exception {
        byte[] id = new byte[24];
        id[0] = 5;
        MessageRecord in =
                MessageRecord.fromMq(id, "{}", 123L, 456L, Map.of("X_From", "MKD", "retry", 5));
        MessageRecord out = MessageRecordCodec.decode(MessageRecordCodec.encode(in));
        assertArrayEquals(in.mqMessageId, out.mqMessageId);
        assertEquals(in.headers, out.headers);
        assertEquals(123L, out.createdDttm);
        assertEquals(456L, out.readedDttm);
        assertEquals("{}", out.payload);
    }

    @Test
    void jmsMessageRoundTrip() throws Exception {
        MessageRecord in = MessageRecord.fromJms("abc", "data", null, null);
        MessageRecord out = MessageRecordCodec.decode(MessageRecordCodec.encode(in));
        assertNull(out.mqMessageId);
        assertEquals("abc", out.jmsMessageId);
        assertEquals("data", out.payload);
    }
}
