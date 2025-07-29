"""
Memory management utilities for cross-platform compatibility
"""
import gc
import platform
import ctypes
import psutil
import os
import logging
import time

from common.logger import Logger
logger = Logger(name=__name__.split('.')[-1], log_dir='logs').get_logger()

def force_memory_cleanup():
    """Cross-platform memory cleanup"""
    # Force garbage collection
    gc.collect()
    
    # Platform-specific memory release
    if platform.system() == "Linux":
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass  # malloc_trim not available
    elif platform.system() == "Windows":
        try:
            # Windows equivalent - use kernel32 to release memory
            kernel32 = ctypes.windll.kernel32
            kernel32.SetProcessWorkingSetSize(-1, -1, -1)
        except Exception:
            pass  # Windows memory management not available

def log_memory_usage(context="", **kwargs):
    """Log current memory usage with optional context and variables"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    
    # Build the log message
    parts = [f"[MEMORY]\t\t{context}\t\tRSS={mem_info.rss/1024/1024:.2f}MB\t\tVMS={mem_info.vms/1024/1024:.2f}MB"]
    
    # Add any additional variables
    for var_name, var_value in kwargs.items():
        if hasattr(var_value, 'qsize'):
            parts.append(f"{var_name}={var_value.qsize()}")
        else:
            parts.append(f"{var_name}={var_value}")
    
    logger.info("\t\t".join(parts))

def monitor_memory_growth(initial_memory=None):
    """Monitor memory growth and return delta"""
    process = psutil.Process(os.getpid())
    current_memory = process.memory_info().rss / 1024 / 1024  # MB
    
    if initial_memory is None:
        return current_memory, 0
    
    delta = current_memory - initial_memory
    return current_memory, delta

def get_queue_state(dict_qName_q):
    m = ""
    for q_nm, q in dict_qName_q.items():
        m += str(q_nm) + ": " + str(q.qsize()) + ", "
    
    return m

class MemoryMonitor:
    """Monitor memory usage over time and detect patterns"""
    
    def __init__(self, log_interval=100):
        self.initial_memory = None
        self.last_memory = None
        self.peak_memory = 0
        self.log_interval = log_interval
        self.counter = 0
        self.start_time = time.time()
    
    def check_memory(self, context=""):
        """Check current memory and log if needed"""
        process = psutil.Process(os.getpid())
        current_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        if self.initial_memory is None:
            self.initial_memory = current_memory
            self.last_memory = current_memory
            print(f"[MEMORY_MONITOR] Initial memory: {current_memory:.2f}MB")
        
        # Track peak memory
        if current_memory > self.peak_memory:
            self.peak_memory = current_memory
        
        # Calculate deltas
        total_delta = current_memory - self.initial_memory
        recent_delta = current_memory - self.last_memory
        
        self.counter += 1
        
        # Log periodically
        if self.counter % self.log_interval == 0:
            elapsed = time.time() - self.start_time
            print(f"[MEMORY_MONITOR] {context} - Current: {current_memory:.2f}MB, "
                  f"Total Δ: {total_delta:+.2f}MB, Recent Δ: {recent_delta:+.2f}MB, "
                  f"Peak: {self.peak_memory:.2f}MB, Elapsed: {elapsed:.1f}s")
        
        self.last_memory = current_memory
        return current_memory, total_delta, recent_delta
    
    def get_summary(self):
        """Get memory usage summary"""
        process = psutil.Process(os.getpid())
        current_memory = process.memory_info().rss / 1024 / 1024  # MB
        elapsed = time.time() - self.start_time
        
        return {
            'initial_memory': self.initial_memory,
            'current_memory': current_memory,
            'peak_memory': self.peak_memory,
            'total_delta': current_memory - self.initial_memory if self.initial_memory else 0,
            'elapsed_time': elapsed,
            'memory_stable': abs(current_memory - self.initial_memory) < 50  # Consider stable if within 50MB
        } 