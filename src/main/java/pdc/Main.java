package pdc;

/**
 * Main class for testing the distributed system.
 */
public class Main {
    public static void main(String[] args) {
        try {
            // Test message protocol
            System.out.println("Testing Message protocol...");
            Message testMsg = new Message(
                "CSM218", 1, "TEST", "test-sender",
                System.currentTimeMillis(), "test payload".getBytes()
            );
            
            byte[] packed = testMsg.pack();
            Message unpacked = Message.unpack(packed);
            unpacked.validate();
            
            System.out.println("Message protocol test passed: " + unpacked);
            
            // Test matrix generation
            System.out.println("Testing matrix generation...");
            int[][] matrix = MatrixGenerator.generateRandomMatrix(3, 3, 42);
            MatrixGenerator.printMatrix(matrix, "Test Matrix:");
            
            System.out.println("All basic tests passed!");
            
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
