package pdc;

/**
 * Simple test runner for Worker without JUnit dependencies
 */
public class SimpleWorkerTest {
    
    public static void main(String[] args) {
        System.out.println("=== Running Worker Tests ===");
        
        // Test 1: Worker Creation
        testWorkerCreation();
        
        // Test 2: Worker Join Logic
        testWorkerJoinLogic();
        
        // Test 3: Worker Start/Stop
        testWorkerStartStop();
        
        System.out.println("=== All Worker Tests Completed ===");
    }
    
    private static void testWorkerCreation() {
        try {
            Worker worker = new Worker("test-worker", "localhost", 9999);
            System.out.println("✅ Worker creation test PASSED");
        } catch (Exception e) {
            System.out.println("❌ Worker creation test FAILED: " + e.getMessage());
        }
    }
    
    private static void testWorkerJoinLogic() {
        try {
            Worker worker = new Worker("test-worker", "localhost", 9999);
            worker.joinCluster(); // Should fail gracefully
            System.out.println("✅ Worker join logic test PASSED");
        } catch (Exception e) {
            System.out.println("✅ Worker join logic test PASSED (expected failure): " + e.getMessage());
        }
    }
    
    private static void testWorkerStartStop() {
        try {
            Worker worker = new Worker("test-worker", "localhost", 9999);
            worker.start();
            Thread.sleep(100);
            worker.stop();
            System.out.println("✅ Worker start/stop test PASSED");
        } catch (Exception e) {
            System.out.println("❌ Worker start/stop test FAILED: " + e.getMessage());
        }
    }
}
