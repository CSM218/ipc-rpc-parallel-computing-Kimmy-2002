#!/usr/bin/env python3

import subprocess
import sys
import os
import time

def run_command(cmd, timeout=30):
    """Run a command with timeout and return result"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"

def test_compilation():
    """Test that Java files compile successfully"""
    print("=== Testing Compilation ===")
    
    # Clean previous build
    run_command("rm -rf build/classes")
    run_command("mkdir -p build/classes")
    
    # Compile all Java files
    ret, stdout, stderr = run_command("javac -d build/classes src/main/java/pdc/*.java")
    
    if ret == 0:
        print("✓ Compilation successful")
        return True
    else:
        print("✗ Compilation failed:")
        print(stderr)
        return False

def test_message_protocol():
    """Test message packing/unpacking"""
    print("\n=== Testing Message Protocol ===")
    
    ret, stdout, stderr = run_command("java -cp build/classes pdc.Main")
    
    if ret == 0 and "All basic tests passed!" in stdout:
        print("✓ Message protocol test passed")
        return True
    else:
        print("✗ Message protocol test failed:")
        print(stdout)
        print(stderr)
        return False

def test_master_worker_basic():
    """Test basic master-worker functionality"""
    print("\n=== Testing Master-Worker Basic Functionality ===")
    
    # Start master in background
    master_cmd = "java -cp build/classes pdc.Master"
    master_proc = subprocess.Popen(master_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Give master time to start
    time.sleep(2)
    
    # Check if master is still running (should be listening)
    if master_proc.poll() is None:
        print("✓ Master started successfully")
        master_proc.terminate()
        master_proc.wait()
        return True
    else:
        print("✗ Master failed to start")
        stdout, stderr = master_proc.communicate()
        print("STDOUT:", stdout.decode())
        print("STDERR:", stderr.decode())
        return False

def main():
    print("CSM218 Distributed Systems Implementation Test")
    print("=" * 50)
    
    # Change to project directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    tests = [
        ("Compilation", test_compilation),
        ("Message Protocol", test_message_protocol),
        ("Master-Worker Basic", test_master_worker_basic),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"Test {test_name} failed")
        except Exception as e:
            print(f"Test {test_name} crashed: {e}")
    
    print(f"\n=== Results ===")
    print(f"Passed: {passed}/{total}")
    print(f"Score: {passed/total * 100:.1f}%")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
