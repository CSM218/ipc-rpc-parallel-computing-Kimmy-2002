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
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    public Message(String magic, int version, String type, String sender, long timestamp, byte[] payload) {
        this.magic = magic;
        this.version = version;
        this.type = type;
        this.sender = sender;
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
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // Write magic string
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(magicBytes.length);
            dos.write(magicBytes);
            
            // Write version
            dos.writeInt(version);
            
            // Write type string
            byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);
            
            // Write sender string
            byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(senderBytes.length);
            dos.write(senderBytes);
            
            // Write timestamp
            dos.writeLong(timestamp);
            
            // Write payload
            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload);
            } else {
                dos.writeInt(0);
            }
            
            byte[] result = baos.toByteArray();
            
            // Prepend total length for framing
            ByteBuffer buffer = ByteBuffer.allocate(4 + result.length);
            buffer.putInt(result.length);
            buffer.put(result);
            
            return buffer.array();
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     * Expects the format used in pack() method.
     */
    public static Message unpack(byte[] data) {
        try {
            // Skip the total length prefix (first 4 bytes)
            ByteArrayInputStream bais = new ByteArrayInputStream(data, 4, data.length - 4);
            DataInputStream dis = new DataInputStream(bais);
            
            Message message = new Message();
            
            // Read magic string
            int magicLength = dis.readInt();
            byte[] magicBytes = new byte[magicLength];
            dis.readFully(magicBytes);
            message.magic = new String(magicBytes, StandardCharsets.UTF_8);
            
            // Read version
            message.version = dis.readInt();
            
            // Read type string
            int typeLength = dis.readInt();
            byte[] typeBytes = new byte[typeLength];
            dis.readFully(typeBytes);
            message.type = new String(typeBytes, StandardCharsets.UTF_8);
            
            // Read sender string
            int senderLength = dis.readInt();
            byte[] senderBytes = new byte[senderLength];
            dis.readFully(senderBytes);
            message.sender = new String(senderBytes, StandardCharsets.UTF_8);
            
            // Read timestamp
            message.timestamp = dis.readLong();
            
            // Read payload
            int payloadLength = dis.readInt();
            if (payloadLength > 0) {
                message.payload = new byte[payloadLength];
                dis.readFully(message.payload);
            } else {
                message.payload = null;
            }
            
            return message;
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
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
