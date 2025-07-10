#!/bin/bash

# /usr Directory Cleanup Script
# This script cleans up old kernel modules, Java versions, and LLVM versions

echo "USR Directory Cleanup Script"
echo "============================"
echo "Date: $(date)"
echo ""

# Check if we're running as root
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run as root (use sudo)"
    exit 1
fi

# Show current /usr size
echo "Current /usr directory size:"
du -sh /usr
echo ""

# Function to clean up old kernel modules
cleanup_kernel_modules() {
    echo "Cleaning up old kernel modules..."
    CURRENT_KERNEL=$(uname -r)
    echo "Current kernel: $CURRENT_KERNEL"
    
    # Keep current kernel and one previous version
    KERNELS_TO_KEEP=2
    OLD_KERNELS=$(ls /usr/lib/modules/ | grep -v "$CURRENT_KERNEL" | sort -V | head -n -$KERNELS_TO_KEEP)
    
    if [ -n "$OLD_KERNELS" ]; then
        echo "Found old kernel modules to remove:"
        echo "$OLD_KERNELS"
        echo ""
        
        for kernel in $OLD_KERNELS; do
            echo "Removing kernel module: $kernel"
            rm -rf "/usr/lib/modules/$kernel"
        done
        echo "Old kernel modules removed."
    else
        echo "No old kernel modules found to remove."
    fi
    echo ""
}

# Function to clean up old Java versions
cleanup_java_versions() {
    echo "Cleaning up old Java versions..."
    CURRENT_JAVA=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    echo "Current Java version: $CURRENT_JAVA"
    
    # Keep current Java version and one previous major version
    JAVA_DIRS=$(ls /usr/lib/jvm/ | grep -E "(java|openjdk)" | sort -V)
    KEEP_COUNT=2
    
    if [ $(echo "$JAVA_DIRS" | wc -l) -gt $KEEP_COUNT ]; then
        OLD_JAVA_DIRS=$(echo "$JAVA_DIRS" | head -n -$KEEP_COUNT)
        echo "Found old Java versions to remove:"
        echo "$OLD_JAVA_DIRS"
        echo ""
        
        for java_dir in $OLD_JAVA_DIRS; do
            echo "Removing Java version: $java_dir"
            rm -rf "/usr/lib/jvm/$java_dir"
        done
        echo "Old Java versions removed."
    else
        echo "No old Java versions found to remove."
    fi
    echo ""
}

# Function to clean up old LLVM versions
cleanup_llvm_versions() {
    echo "Cleaning up old LLVM versions..."
    LLVM_DIRS=$(ls /usr/lib/llvm-* 2>/dev/null | sort -V)
    
    if [ -n "$LLVM_DIRS" ] && [ $(echo "$LLVM_DIRS" | wc -l) -gt 2 ]; then
        # Keep the two most recent LLVM versions
        OLD_LLVM_DIRS=$(echo "$LLVM_DIRS" | head -n -2)
        echo "Found old LLVM versions to remove:"
        echo "$OLD_LLVM_DIRS"
        echo ""
        
        for llvm_dir in $OLD_LLVM_DIRS; do
            echo "Removing LLVM version: $llvm_dir"
            rm -rf "$llvm_dir"
        done
        echo "Old LLVM versions removed."
    else
        echo "No old LLVM versions found to remove."
    fi
    echo ""
}

# Function to clean up package cache
cleanup_package_cache() {
    echo "Cleaning up package cache..."
    if command -v apt-get &> /dev/null; then
        echo "Cleaning apt cache..."
        apt-get clean
        apt-get autoremove -y
    fi
    
    if command -v dnf &> /dev/null; then
        echo "Cleaning dnf cache..."
        dnf clean all
    fi
    
    if command -v yum &> /dev/null; then
        echo "Cleaning yum cache..."
        yum clean all
    fi
    echo ""
}

# Run cleanup functions
cleanup_kernel_modules
cleanup_java_versions
cleanup_llvm_versions
cleanup_package_cache

# Show final /usr size
echo "Final /usr directory size:"
du -sh /usr

echo ""
echo "Cleanup completed successfully!" 