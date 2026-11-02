package com.gpb.datafirewall.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Explicit format for messages waiting at the very first cache bootstrap. */
public final class MessageRecordCodec {

    private static final int MAGIC = 0x4D524331;
    private static final int MAX_BYTES = 32*1024*1024;

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    private MessageRecordCodec() {}

    public static byte[] encode(MessageRecord msg) throws IOException {

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);

        out.writeInt(MAGIC);

        if (msg.mqMessageId == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(msg.mqMessageId.length);
            out.write(msg.mqMessageId);
        }

        writeString(out, msg.jmsMessageId);
        writeString(out, msg.payload);
        writeLong(out, msg.createdDttm);
        writeLong(out, msg.readedDttm);

        byte[] headers = msg.headers == null ? null : MAPPER.writeValueAsBytes(msg.headers);

        if (headers==null)
            out.writeInt(-1);
        else {
            out.writeInt(headers.length);out.write(headers);
        }

        out.flush();

        if (bytes.size() > MAX_BYTES)
            throw new IOException("Pending message too large");

        return bytes.toByteArray();
    }

    public static MessageRecord decode(byte[] bytes)throws IOException {

        DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(bytes)
        );

        if (in.readInt() != MAGIC)
            throw new IOException("Bad buffered message magic");

        int idlen = in.readInt();
        byte[] id = null;

        if (idlen>=0) {
            if (idlen != MessageRecord.MQ_ID_LEN)
                throw new IOException("Bad MQ ID length");

            id = new byte[idlen];
            in.readFully(id);
        }

        String jmsId = readString(in),
                payload = readString(in);

        Long created = readLong(in),
                read = readLong(in);

        int hn = in.readInt();

        Map<String,Object> headers = null;

        if (hn >= 0) {
            if (hn > MAX_BYTES)
                throw new IOException("Headers too large");
            
            byte[] h = new byte[hn];

            in.readFully(h);

            headers = MAPPER.readValue(
                    h,
                    new TypeReference<Map<String,Object>>(){}
            );
        }

        if (in.available() != 0)
            throw new IOException("Trailing buffered message");

        return id != null 
                ? MessageRecord.fromMq(
                        id,
                        payload,
                        created,
                        read,
                        headers
                ) 
                : MessageRecord.fromJms(
                        jmsId,
                        payload,
                        created,
                        read
                );
    }

    private static void writeLong(
            DataOutputStream out,
            Long v) throws IOException {
        out.writeBoolean(v != null);

        if (v != null)
            out.writeLong(v);
    }

    private static Long readLong(
            DataInputStream in) throws IOException {
        return in.readBoolean() ? in.readLong() : null;
    }

    private static void writeString(
            DataOutputStream out,
            String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        
        byte[] b = s.getBytes(
                StandardCharsets.UTF_8
        );
        
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(
            DataInputStream in) throws IOException {
        int n = in.readInt();

        if (n < 0) return null;

        if(n > MAX_BYTES)
            throw new IOException("String too large");

        byte[] b = new byte[n];

        in.readFully(b);
        
        return new String(b, StandardCharsets.UTF_8);
    }
}
