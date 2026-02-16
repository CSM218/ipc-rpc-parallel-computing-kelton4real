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

    public static final String CONNECT = "CONNECT";
    public static final String REGISTER_WORKER = "REGISTER_WORKER";
    public static final String REGISTER_CAPABILITIES = "REGISTER_CAPABILITIES";
    public static final String RPC_REQUEST = "RPC_REQUEST";
    public static final String RPC_RESPONSE = "RPC_RESPONSE";
    public static final String TASK_COMPLETE = "TASK_COMPLETE";
    public static final String TASK_ERROR = "TASK_ERROR";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String WORKER_ACK = "WORKER_ACK";

    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public String payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.messageType = CONNECT;
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "anonymous");
        this.timestamp = System.currentTimeMillis();
        this.payload = "";
    }

    public byte[] serialize() {
        return pack();
    }

    public byte[] pack() {
        validate();
        try {
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            byte[] typeBytes = messageType.getBytes(StandardCharsets.UTF_8);
            byte[] studentBytes = studentId.getBytes(StandardCharsets.UTF_8);
            byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);

            if (magicBytes.length > MAX_FIELD_BYTES || typeBytes.length > MAX_FIELD_BYTES || studentBytes.length > MAX_FIELD_BYTES) {
                throw new IllegalArgumentException("Message header field too large");
            }
            if (payloadBytes.length > MAX_PAYLOAD_BYTES) {
                throw new IllegalArgumentException("Payload too large");
            }

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
            throw new IllegalStateException("Failed to serialize message", e);
        }
    }

    public static Message deserialize(byte[] data) {
        return unpack(data);
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
            message.payload = new String(readBoundedField(in, MAX_PAYLOAD_BYTES), StandardCharsets.UTF_8);
            message.validate();

            return message;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to unpack message", e);
        }
    }

    public String toJson() {
        validate();
        return "{"
                + "\"magic\":\"" + escape(magic) + "\","
                + "\"version\":" + version + ","
                + "\"messageType\":\"" + escape(messageType) + "\","
                + "\"studentId\":\"" + escape(studentId) + "\","
                + "\"timestamp\":" + timestamp + ","
                + "\"payload\":\"" + escape(payload) + "\""
                + "}";
    }

    public static Message parse(String json) {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("json cannot be null or empty");
        }

        Message message = new Message();
        message.magic = extractString(json, "magic", "CSM218");
        message.version = (int) extractLong(json, "version", 1);
        message.messageType = extractString(json, "messageType", CONNECT);
        message.studentId = extractString(json, "studentId", "anonymous");
        message.timestamp = extractLong(json, "timestamp", System.currentTimeMillis());
        message.payload = extractString(json, "payload", "");
        message.validate();
        return message;
    }

    public void validate() {
        if (magic == null || !"CSM218".equals(magic)) {
            throw new IllegalArgumentException("Invalid magic");
        }
        if (version < 1) {
            throw new IllegalArgumentException("Invalid version");
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalArgumentException("Invalid messageType");
        }
        if (studentId == null || studentId.isEmpty()) {
            throw new IllegalArgumentException("Invalid studentId");
        }
        if (timestamp <= 0) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
        if (payload == null) {
            payload = "";
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

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String extractString(String json, String key, String defaultValue) {
        String pattern = "\"" + key + "\"";
        int keyIndex = json.indexOf(pattern);
        if (keyIndex < 0) {
            return defaultValue;
        }
        int colonIndex = json.indexOf(':', keyIndex + pattern.length());
        if (colonIndex < 0) {
            return defaultValue;
        }
        int firstQuote = json.indexOf('"', colonIndex + 1);
        if (firstQuote < 0) {
            return defaultValue;
        }
        int secondQuote = firstQuote + 1;
        while (secondQuote < json.length()) {
            secondQuote = json.indexOf('"', secondQuote);
            if (secondQuote < 0) {
                return defaultValue;
            }
            if (json.charAt(secondQuote - 1) != '\\') {
                break;
            }
            secondQuote++;
        }
        String raw = json.substring(firstQuote + 1, secondQuote);
        return raw.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static long extractLong(String json, String key, long defaultValue) {
        String pattern = "\"" + key + "\"";
        int keyIndex = json.indexOf(pattern);
        if (keyIndex < 0) {
            return defaultValue;
        }
        int colonIndex = json.indexOf(':', keyIndex + pattern.length());
        if (colonIndex < 0) {
            return defaultValue;
        }
        int start = colonIndex + 1;
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        if (start == end) {
            return defaultValue;
        }
        try {
            return Long.parseLong(json.substring(start, end));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
