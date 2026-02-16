package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Message {
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.messageType = "UNKNOWN";
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "anonymous");
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
    }

    public byte[] serialize() {
        return pack();
    }

    public byte[] pack() {
        try {
            byte[] magicBytes = magic == null ? new byte[0] : magic.getBytes(StandardCharsets.UTF_8);
            byte[] typeBytes = messageType == null ? new byte[0] : messageType.getBytes(StandardCharsets.UTF_8);
            byte[] studentBytes = studentId == null ? new byte[0] : studentId.getBytes(StandardCharsets.UTF_8);
            byte[] payloadBytes = payload == null ? new byte[0] : payload;

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);

            out.writeInt(magicBytes.length);
            out.write(magicBytes);
            out.writeInt(version);
            out.writeInt(typeBytes.length);
            out.write(typeBytes);
            out.writeInt(studentBytes.length);
            out.write(studentBytes);
            out.writeLong(timestamp);
            out.writeInt(payloadBytes.length);
            out.write(payloadBytes);
            out.flush();

            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    public static Message unpack(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }

        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            Message message = new Message();

            int magicLen = in.readInt();
            byte[] magicBytes = new byte[Math.max(magicLen, 0)];
            in.readFully(magicBytes);
            message.magic = new String(magicBytes, StandardCharsets.UTF_8);
            message.version = in.readInt();

            int typeLen = in.readInt();
            byte[] typeBytes = new byte[Math.max(typeLen, 0)];
            in.readFully(typeBytes);
            message.messageType = new String(typeBytes, StandardCharsets.UTF_8);

            int studentLen = in.readInt();
            byte[] studentBytes = new byte[Math.max(studentLen, 0)];
            in.readFully(studentBytes);
            message.studentId = new String(studentBytes, StandardCharsets.UTF_8);

            message.timestamp = in.readLong();

            int payloadLen = in.readInt();
            byte[] payloadBytes = new byte[Math.max(payloadLen, 0)];
            in.readFully(payloadBytes);
            message.payload = payloadBytes;

            if (!"CSM218".equals(message.magic)) {
                throw new IllegalArgumentException("Invalid magic");
            }
            if (message.version < 1) {
                throw new IllegalArgumentException("Invalid version");
            }

            return message;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to unpack message", e);
        }
    }
}
