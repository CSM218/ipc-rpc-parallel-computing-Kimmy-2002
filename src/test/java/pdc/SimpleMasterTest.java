package pdc;

/**
 * Simple test runner for Master without JUnit dependencies
 */
public class SimpleMasterTest {
    
    public static void main(String[] args) {
        System.out.println("=== Running Master Tests ===");
        
        // Test 1: Master Creation
        testMasterCreation();
        
        // Test 2: Master Coordinate
        testMasterCoordinate();
        
        // Test 3: Master Reconcile State
        testMasterReconcileState();
        
        System.out.println("=== All Master Tests Completed ===");
    }
    
    private static void testMasterCreation() {
        try {
            Master master = new Master();
            System.out.println("✅ Master creation test PASSED");
        } catch (Exception e) {
            System.out.println("❌ Master creation test FAILED: " + e.getMessage());
        }
    }
    
    private static void testMasterCoordinate() {
        try {
            Master master = new Master();
            int[][] matrix = { { 1, 2 }, { 3, 4 } };
            
            try {
                Object result = master.coordinate("BLOCK_MULTIPLY", matrix, 1);
                System.out.println("✅ Master coordinate test PASSED (result: " + result + ")");
            } catch (IllegalStateException e) {
                System.out.println("✅ Master coordinate test PASSED (expected no workers): " + e.getMessage());
            }
        } catch (Exception e) {
            System.out.println("❌ Master coordinate test FAILED: " + e.getMessage());
        }
    }
    
    private static void testMasterReconcileState() {
        try {
            Master master = new Master();
            master.reconcileState();
            System.out.println("✅ Master reconcile state test PASSED");
        } catch (Exception e) {
            System.out.println("❌ Master reconcile state test FAILED: " + e.getMessage());
        }
    }
}
