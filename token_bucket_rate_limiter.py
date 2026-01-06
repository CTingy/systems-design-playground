import threading
import time


class RateLimiter:
    def __init__(self, capacity, fill_rate):
        self.capacity = capacity
        self.fill_rate = fill_rate  # tokens generated per second
        self.tokens = capacity
        self.last_fill_time = time.time()
        self.lock = threading.Lock()

    def allow_request(self, tokens_needed=1):
        with self.lock:
            now = time.time()

            # Refill tokens based on the elapsed time
            elapsed = now - self.last_fill_time
            self.tokens = min(
                self.capacity,
                self.tokens + elapsed * self.fill_rate
            )
            self.last_fill_time = now

            if self.tokens >= tokens_needed:
                self.tokens -= tokens_needed
                return True

            return False
