from collections import deque
import time

class RateLimiter:
    def __init__(self, limit, interval):
        self.limit = limit  # Maximum number of messages allowed in the interval
        self.interval = interval  # Time interval in seconds
        self.timestamps = deque(maxlen=limit)

    def can_send_message(self):
        now = time.time()
        
        # Remove timestamps that are older than the interval
        while self.timestamps and now - self.timestamps[0] > self.interval:
            self.timestamps.popleft()
        
        # Check if the number of messages sent within the interval exceeds the limit
        return len(self.timestamps) < self.limit

    def send_message(self):
        if self.can_send_message():
            self.timestamps.append(time.time())