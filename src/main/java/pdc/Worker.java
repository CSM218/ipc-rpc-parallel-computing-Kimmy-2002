package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
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
            // Establish connection to master
            masterConnection = new Socket(masterHost, masterPort);
            input = new DataInputStream(masterConnection.getInputStream());
            output = new DataOutputStream(masterConnection.getOutputStream());
            
            // Send registration message
            Message registerMsg = new Message(
                "CSM218", 1, "REGISTER_WORKER", workerId, 
                System.currentTimeMillis(), 
                workerId.getBytes()
            );
            output.write(registerMsg.pack());
            output.flush();
            
            // Wait for acknowledgment
            byte[] responseBytes = readMessage();
            Message response = Message.unpack(responseBytes);
            response.validate();
            
            if ("WORKER_ACK".equals(response.type)) {
                System.out.println("Worker " + workerId + " registered successfully");
                running.set(true);
                
                // Send capability announcement
                Message capabilityMsg = new Message(
                    "CSM218", 1, "REGISTER_CAPABILITIES", workerId,
                    System.currentTimeMillis(),
                    "MATRIX_MULTIPLICATION:4".getBytes() // 4 concurrent threads
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
                    case "RPC_REQUEST":
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
                    "CSM218", 1, "TASK_COMPLETE", workerId,
                    System.currentTimeMillis(), result
                );
                
                synchronized (output) {
                    output.write(response.pack());
                    output.flush();
                }
                
            } catch (Exception e) {
                // Send error response
                Message errorResponse = new Message(
                    "CSM218", 1, "TASK_ERROR", workerId,
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
            // Parse task data (simplified - expecting matrix multiplication task)
            String taskString = new String(taskData);
            String[] parts = taskString.split(":");
            
            if (parts.length < 3) {
                throw new IllegalArgumentException("Invalid task format");
            }
            
            String operation = parts[0];
            int matrixSize = Integer.parseInt(parts[1]);
            int seed = Integer.parseInt(parts[2]);
            
            if ("BLOCK_MULTIPLY".equals(operation)) {
                // Generate test matrices and perform multiplication
                int[][] matrixA = MatrixGenerator.generateRandomMatrix(matrixSize, matrixSize, seed);
                int[][] matrixB = MatrixGenerator.generateRandomMatrix(matrixSize, matrixSize, seed + 1);
                int[][] result = multiplyMatrices(matrixA, matrixB);
                
                long endTime = System.currentTimeMillis();
                System.out.println("Worker " + workerId + " completed " + matrixSize + "x" + matrixSize + 
                                 " multiplication in " + (endTime - startTime) + "ms");
                
                // Serialize result
                return serializeMatrix(result);
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
                "CSM218", 1, "HEARTBEAT_ACK", workerId,
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
            // Read message length
            int length = input.readInt();
            if (length <= 0) {
                return null;
            }
            
            // Read message data
            byte[] data = new byte[length];
            input.readFully(data);
            
            return data;
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
}
