package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;
    public int version;
    public String type;
    public String messageType;
    public String sender;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    public Message(String magic, int version, String type, String messageType, String sender, String studentId, long timestamp, byte[] payload) {
        this.magic = magic;
        this.version = version;
        this.type = type;
        this.messageType = messageType;
        this.sender = sender;
        this.studentId = studentId;
        this.timestamp = timestamp;
        this.payload = payload;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses length-prefixed binary format for efficient parsing.
     * Format: [total_length][magic_length][magic][version][type_length][type][sender_length][sender][timestamp][payload_length][payload]
     */
    public byte[] pack() {
        try {
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
            byte[] messageTypeBytes = messageType != null ? messageType.getBytes(StandardCharsets.UTF_8) : new byte[0];
            byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
            byte[] studentIdBytes = studentId != null ? studentId.getBytes(StandardCharsets.UTF_8) : new byte[0];
            int payloadLength = (payload != null) ? payload.length : 0;

            int totalLength = 4 + magicBytes.length + 4 + 4 + typeBytes.length + 4 + messageTypeBytes.length + 4 + senderBytes.length + 4 + studentIdBytes.length + 8 + 4 + payloadLength;
            ByteBuffer buffer = ByteBuffer.allocate(totalLength + 4); // +4 for outer frame length

            buffer.putInt(totalLength); // Outer frame length

            buffer.putInt(magicBytes.length);
            buffer.put(magicBytes);
            buffer.putInt(version);
            buffer.putInt(typeBytes.length);
            buffer.put(typeBytes);
            buffer.putInt(messageTypeBytes.length);
            buffer.put(messageTypeBytes);
            buffer.putInt(senderBytes.length);
            buffer.put(senderBytes);
            buffer.putInt(studentIdBytes.length);
            buffer.put(studentIdBytes);
            buffer.putLong(timestamp);
            buffer.putInt(payloadLength);
            if (payload != null) {
                buffer.put(payload);
            }

            return buffer.array();
        } catch (Exception e) {
            throw new RuntimeException("Failed to pack message with ByteBuffer", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Expects the format used in pack() method.
     */
    public static Message unpack(byte[] data) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int totalLength = buffer.getInt(); // Read outer frame length

            if (totalLength != data.length - 4) {
                 throw new IOException("Corrupted frame: expected " + totalLength + " bytes, got " + (data.length - 4));
            }

            Message message = new Message();
            
            int magicLength = buffer.getInt();
            byte[] magicBytes = new byte[magicLength];
            buffer.get(magicBytes);
            message.magic = new String(magicBytes, StandardCharsets.UTF_8);

            message.version = buffer.getInt();

            int typeLength = buffer.getInt();
            byte[] typeBytes = new byte[typeLength];
            buffer.get(typeBytes);
            message.type = new String(typeBytes, StandardCharsets.UTF_8);

            int messageTypeLength = buffer.getInt();
            if (messageTypeLength > 0) {
                byte[] messageTypeBytes = new byte[messageTypeLength];
                buffer.get(messageTypeBytes);
                message.messageType = new String(messageTypeBytes, StandardCharsets.UTF_8);
            } else {
                message.messageType = "";
            }
            
            int senderLength = buffer.getInt();
            byte[] senderBytes = new byte[senderLength];
            buffer.get(senderBytes);
            message.sender = new String(senderBytes, StandardCharsets.UTF_8);

            int studentIdLength = buffer.getInt();
             if (studentIdLength > 0) {
                byte[] studentIdBytes = new byte[studentIdLength];
                buffer.get(studentIdBytes);
                message.studentId = new String(studentIdBytes, StandardCharsets.UTF_8);
            } else {
                message.studentId = "";
            }

            message.timestamp = buffer.getLong();

            int payloadLength = buffer.getInt();
            if (payloadLength > 0) {
                message.payload = new byte[payloadLength];
                buffer.get(message.payload);
            } else {
                message.payload = null;
            }
            
            return message;
        } catch (Exception e) {
            throw new RuntimeException("Failed to unpack message with ByteBuffer", e);
        }
    }

    public static byte[] readFramedMessage(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > 10 * 1024 * 1024) { // 10MB limit
            throw new IOException("Invalid message length: " + length);
        }
        byte[] messageBytes = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int result = in.read(messageBytes, bytesRead, length - bytesRead);
            if (result == -1) {
                throw new IOException("Unexpected end of stream while reading message payload.");
            }
            bytesRead += result;
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(4 + length);
        buffer.putInt(length);
        buffer.put(messageBytes);
        return buffer.array();
    }

    /**
     * Validates that the message follows the CSM218 protocol.
     */
    public void validate() throws Exception {
        if (magic == null || !magic.equals("CSM218")) {
            throw new Exception("Invalid magic number: " + magic);
        }
        if (version != 1) {
            throw new Exception("Unsupported version: " + version);
        }
        if (type == null || type.isEmpty()) {
            throw new Exception("Message type cannot be null or empty");
        }
        if (sender == null || sender.isEmpty()) {
            throw new Exception("Sender cannot be null or empty");
        }
        if (timestamp <= 0) {
            throw new Exception("Invalid timestamp: " + timestamp);
        }
    }

    @Override
    public String toString() {
        return String.format("Message{magic='%s', version=%d, type='%s', sender='%s', timestamp=%d, payloadLength=%d}",
                magic, version, type, sender, timestamp, payload != null ? payload.length : 0);
    }
}
