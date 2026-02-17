package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    private final String workerId;
    private final String masterHost;
    private final int masterPort;
    private final ExecutorService taskExecutor;
    private final AtomicBoolean running;
    private Socket masterConnection;
    private DataInputStream input;
    private DataOutputStream output;

    public Worker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.taskExecutor = Executors.newFixedThreadPool(4); // 4 concurrent tasks
        this.running = new AtomicBoolean(false);
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster() {
        try {
            // Establish connection to master with timeout
            masterConnection = new Socket();
            masterConnection.connect(new InetSocketAddress(masterHost, masterPort), 3000); // 3 second timeout
            masterConnection.setSoTimeout(5000); // 5 second read timeout
            input = new DataInputStream(masterConnection.getInputStream());
            output = new DataOutputStream(masterConnection.getOutputStream());
            
            // Send registration message
            Message registerMsg = new Message(
                "CSM218", 1, "REGISTER_WORKER", "REGISTRATION", workerId, 
                System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "UNKNOWN", 
                System.currentTimeMillis(), 
                workerId.getBytes()
            );
            output.write(registerMsg.pack());
            output.flush();
            
            // Wait for acknowledgment
            byte[] responseBytes = readMessage();
            Message response = Message.unpack(responseBytes);
            response.validate();
            
            // Enhanced handshake validation
            if (!"CSM218".equals(response.magic) || response.version != 1) {
                throw new IOException("Protocol version mismatch");
            }
            
            if ("WORKER_ACK".equals(response.type)) {
                System.out.println("Worker " + workerId + " registered successfully");
                running.set(true);
                
                // Send enhanced capability announcement with protocol info
                Message capabilityMsg = new Message(
                    "CSM218", 1, "REGISTER_CAPABILITIES", "CAPABILITY", workerId,
                    System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "UNKNOWN",
                    System.currentTimeMillis(),
                    ("MATRIX_MULTIPLICATION:4:PROTOCOL_V2:ENHANCED_HANDSHAKE").getBytes()
                );
                output.write(capabilityMsg.pack());
                output.flush();
                
                // Start message processing loop
                run();
            } else {
                throw new IOException("Registration failed: " + response.type);
            }
            
        } catch (Exception e) {
            System.err.println("Failed to join cluster: " + e.getMessage());
            cleanup();
        }
    }

    /**
     * Starts the worker's main processing loop.
     */
    public void execute() {
        if (!running.get()) {
            System.err.println("Worker not connected. Call joinCluster() first.");
            return;
        }
        run();
    }

    /**
     * Main event loop for processing messages from master.
     */
    private void run() {
        while (running.get()) {
            try {
                byte[] messageBytes = readMessage();
                if (messageBytes == null) break;
                
                Message message = Message.unpack(messageBytes);
                message.validate();
                
                // Handle different message types
                switch (message.type) {
                    case "TASK_REQUEST":
                        handleTaskRequest(message);
                        break;
                    case "HEARTBEAT":
                        handleHeartbeat(message);
                        break;
                    default:
                        System.err.println("Unknown message type: " + message.type);
                }
                
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
                if (running.get()) {
                    // Continue processing other messages
                    continue;
                }
            }
        }
    }

    /**
     * Handles incoming task requests from master.
     */
    private void handleTaskRequest(Message request) {
        taskExecutor.submit(() -> {
            try {
                // Execute the task
                byte[] result = executeTask(request.payload);
                
                // Send result back to master
                Message response = new Message(
                    "CSM218", 1, "TASK_COMPLETE", "RESULT", workerId,
                    System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "UNKNOWN",
                    System.currentTimeMillis(), result
                );
                
                synchronized (output) {
                    output.write(response.pack());
                    output.flush();
                }
                
            } catch (Exception e) {
                // Send error response
                Message errorResponse = new Message(
                    "CSM218", 1, "TASK_ERROR", "ERROR", workerId,
                    System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "UNKNOWN",
                    System.currentTimeMillis(), 
                    e.getMessage().getBytes()
                );
                
                try {
                    synchronized (output) {
                        output.write(errorResponse.pack());
                        output.flush();
                    }
                } catch (IOException ioException) {
                    System.err.println("Failed to send error response: " + ioException.getMessage());
                }
            }
        });
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    private byte[] executeTask(byte[] taskData) {
        long startTime = System.currentTimeMillis();
        
        try {
            // Parse task data (expecting matrix multiplication task)
            String taskString = new String(taskData);
            String[] parts = taskString.split(":");
            
            if (parts.length < 5) {
                throw new IllegalArgumentException("Invalid task format: expected at least 5 parts, got " + parts.length);
            }
            
            String operation = parts[0];
            int matrixSize = Integer.parseInt(parts[1]);
            int startRow = Integer.parseInt(parts[2]);
            int endRow = Integer.parseInt(parts[3]);
            int seed = Integer.parseInt(parts[4]);
            
            if ("BLOCK_MULTIPLY".equals(operation)) {
                // Generate test matrices and perform multiplication
                int[][] matrixA = MatrixGenerator.generateRandomMatrix(matrixSize, matrixSize, seed);
                int[][] matrixB = MatrixGenerator.generateRandomMatrix(matrixSize, matrixSize, seed + 1);
                int[][] result = multiplyMatrices(matrixA, matrixB);
                
                // Extract the rows that this worker is responsible for
                int blockHeight = endRow - startRow;
                int[][] blockResult = new int[blockHeight][matrixSize];
                for (int i = 0; i < blockHeight; i++) {
                    System.arraycopy(result[startRow + i], 0, blockResult[i], 0, matrixSize);
                }
                
                long endTime = System.currentTimeMillis();
                System.out.println("Worker " + workerId + " completed block [" + startRow + "-" + endRow + 
                                 "] of " + matrixSize + "x" + matrixSize + " multiplication in " + (endTime - startTime) + "ms");
                
                // Serialize block result
                return serializeMatrix(blockResult);
            } else {
                throw new UnsupportedOperationException("Unknown operation: " + operation);
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Task execution failed", e);
        }
    }

    /**
     * Simple matrix multiplication implementation.
     */
    private int[][] multiplyMatrices(int[][] a, int[][] b) {
        int n = a.length;
        int[][] result = new int[n][n];
        
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                for (int k = 0; k < n; k++) {
                    result[i][j] += a[i][k] * b[k][j];
                }
            }
        }
        
        return result;
    }

    /**
     * Serializes a matrix to byte array.
     */
    private byte[] serializeMatrix(int[][] matrix) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
            
            dos.writeInt(matrix.length);
            dos.writeInt(matrix[0].length);
            
            for (int[] row : matrix) {
                for (int val : row) {
                    dos.writeInt(val);
                }
            }
            
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize matrix", e);
        }
    }

    /**
     * Handles heartbeat messages from master.
     */
    private void handleHeartbeat(Message heartbeat) {
        try {
            Message heartbeatResponse = new Message(
                "CSM218", 1, "HEARTBEAT_ACK", "HEARTBEAT", workerId,
                System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "UNKNOWN",
                System.currentTimeMillis(), null
            );
            
            synchronized (output) {
                output.write(heartbeatResponse.pack());
                output.flush();
            }
        } catch (IOException e) {
            System.err.println("Failed to send heartbeat response: " + e.getMessage());
        }
    }

    /**
     * Reads a complete message from the input stream.
     */
    private byte[] readMessage() throws IOException {
        try {
            // Read message length with timeout
            int length = input.readInt();
            if (length <= 0 || length > 1000000) { // Max 1MB message
                return null;
            }
            
            // Read message data
            byte[] messageData = new byte[length];
            input.readFully(messageData);
            
            // Prepend the total length for Message.unpack() to skip
            byte[] completePacket = new byte[length + 4];
            System.arraycopy(ByteBuffer.allocate(4).putInt(length).array(), 0, completePacket, 0, 4);
            System.arraycopy(messageData, 0, completePacket, 4, length);
            
            return completePacket;
        } catch (IOException e) {
            running.set(false);
            throw e;
        }
    }

    /**
     * Cleanup resources.
     */
    private void cleanup() {
        running.set(false);
        
        try {
            if (taskExecutor != null) {
                taskExecutor.shutdown();
            }
            if (input != null) {
                input.close();
            }
            if (output != null) {
                output.close();
            }
            if (masterConnection != null) {
                masterConnection.close();
            }
        } catch (IOException e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }

    /**
     * Public method to start the worker.
     */
    public void start() {
        joinCluster();
    }

    /**
     * Public method to stop the worker.
     */
    public void stop() {
        cleanup();
    }

    /**
     * Main method to start a worker.
     */
    public static void main(String[] args) {
        try {
            String workerId = "worker-" + System.currentTimeMillis();
            String masterHost = "localhost";
            int masterPort = 9999;
            
            // Parse command line arguments
            if (args.length >= 1) {
                workerId = args[0];
            }
            if (args.length >= 2) {
                masterHost = args[1];
            }
            if (args.length >= 3) {
                masterPort = Integer.parseInt(args[2]);
            }
            
            // Check environment variables
            String envWorkerId = System.getenv("WORKER_ID");
            String envMasterHost = System.getenv("MASTER_HOST");
            String envMasterPort = System.getenv("MASTER_PORT");
            
            if (envWorkerId != null) {
                workerId = envWorkerId;
            }
            if (envMasterHost != null) {
                masterHost = envMasterHost;
            }
            if (envMasterPort != null) {
                masterPort = Integer.parseInt(envMasterPort);
            }
            
            Worker worker = new Worker(workerId, masterHost, masterPort);
            final String finalWorkerId = workerId; // Make it effectively final
            
            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down worker " + finalWorkerId + "...");
                worker.stop();
            }));
            
            System.out.println("Starting worker " + workerId + " connecting to " + masterHost + ":" + masterPort);
            worker.start();
            
        } catch (Exception e) {
            System.err.println("Worker failed to start: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
