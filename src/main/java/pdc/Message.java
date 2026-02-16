package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Message {
    private static final int MAX_FIELD_BYTES = 64 * 1024;
    private static final int MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;

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

    public static Message deserialize(byte[] data) {
        return unpack(data);
    }

    public byte[] pack() {
        validateForPack();
        try {
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            byte[] typeBytes = messageType.getBytes(StandardCharsets.UTF_8);
            byte[] studentBytes = studentId.getBytes(StandardCharsets.UTF_8);
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
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("data cannot be null or empty");
        }

        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            Message message = new Message();

            message.magic = new String(readBoundedField(in, MAX_FIELD_BYTES), StandardCharsets.UTF_8);
            message.version = in.readInt();
            message.messageType = new String(readBoundedField(in, MAX_FIELD_BYTES), StandardCharsets.UTF_8);
            message.studentId = new String(readBoundedField(in, MAX_FIELD_BYTES), StandardCharsets.UTF_8);
            message.timestamp = in.readLong();
            message.payload = readBoundedField(in, MAX_PAYLOAD_BYTES);

            message.validateForUse();
            return message;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to unpack message", e);
        }
    }

    private static byte[] readBoundedField(DataInputStream in, int maxLength) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maxLength) {
            throw new IllegalArgumentException("Invalid field length: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private void validateForPack() {
        if (magic == null || messageType == null || studentId == null) {
            throw new IllegalArgumentException("Message fields cannot be null");
        }
        if (payload == null) {
            payload = new byte[0];
        }
        validateForUse();
    }

    private void validateForUse() {
        if (!"CSM218".equals(magic)) {
            throw new IllegalArgumentException("Invalid magic");
        }
        if (version < 1) {
            throw new IllegalArgumentException("Invalid version");
        }
        if (messageType.isEmpty()) {
            throw new IllegalArgumentException("Invalid messageType");
        }
        if (studentId.isEmpty()) {
            throw new IllegalArgumentException("Invalid studentId");
        }
        if (timestamp <= 0) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Payload too large");
        }
    }
}
