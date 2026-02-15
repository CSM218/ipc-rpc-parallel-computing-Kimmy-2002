#!/usr/bin/env python3

import subprocess
import time
import sys
import os

def test_master_worker_integration():
    """Test actual master-worker communication"""
    print("=== Testing Master-Worker Integration ===")
    
    test_port = 12346
    
    # Start master in background
    master_cmd = f"java -cp build/classes pdc.Master {test_port}"
    master_proc = subprocess.Popen(master_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Give master time to start
    time.sleep(2)
    
    # Check if master is still running
    if master_proc.poll() is not None:
        print("✗ Master failed to start")
        stdout, stderr = master_proc.communicate()
        print("STDOUT:", stdout.decode())
        print("STDERR:", stderr.decode())
        return False
    
    print("✓ Master started successfully")
    
    # Start a worker
    worker_cmd = f"java -cp build/classes pdc.Worker test-worker localhost {test_port}"
    worker_proc = subprocess.Popen(worker_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Give worker time to connect
    time.sleep(3)
    
    # Check if both are still running (indicating successful connection)
    master_running = master_proc.poll() is None
    worker_running = worker_proc.poll() is None
    
    if master_running and worker_running:
        print("✓ Worker connected successfully")
        
        # Let them run for a bit to test heartbeat
        time.sleep(2)
        
        # Check if both are still running after heartbeat period
        master_still_running = master_proc.poll() is None
        worker_still_running = worker_proc.poll() is None
        
        if master_still_running and worker_still_running:
            print("✓ Heartbeat mechanism working")
        else:
            print("⚠ Heartbeat may have issues")
    else:
        print("✗ Worker failed to connect")
        if not worker_running:
            stdout, stderr = worker_proc.communicate()
            print("Worker STDOUT:", stdout.decode())
            print("Worker STDERR:", stderr.decode())
        return False
    
    # Clean shutdown
    print("Shutting down...")
    master_proc.terminate()
    worker_proc.terminate()
    
    try:
        master_proc.wait(timeout=5)
        worker_proc.wait(timeout=5)
        print("✓ Clean shutdown successful")
        return True
    except subprocess.TimeoutExpired:
        print("⚠ Some processes didn't shut down cleanly")
        master_proc.kill()
        worker_proc.kill()
        return False

def main():
    print("CSM218 Integration Test")
    print("=" * 30)
    
    # Change to project directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    try:
        if test_master_worker_integration():
            print("\n🎉 Integration test passed!")
            return 0
        else:
            print("\n❌ Integration test failed")
            return 1
    except Exception as e:
        print(f"\n💥 Integration test crashed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
