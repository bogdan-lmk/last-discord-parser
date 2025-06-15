# app/utils/rate_limiter.py
import asyncio
import time
from typing import Dict, Optional, Any
from dataclasses import dataclass

@dataclass
class RateLimitBucket:
    """Rate limiting bucket for tracking requests"""
    requests: int = 0
    reset_time: float = 0
    window_seconds: float = 60  # Default 1 minute window

class RateLimiter:
    """Advanced rate limiter with multiple strategies and timeout protection"""
    
    def __init__(self, 
                 requests_per_second: Optional[float] = None,
                 requests_per_minute: Optional[int] = None,
                 name: str = "default"):
        self.name = name
        self.requests_per_second = requests_per_second
        self.requests_per_minute = requests_per_minute
        
        # Tracking buckets
        self.buckets: Dict[str, RateLimitBucket] = {}
        self._lock = asyncio.Lock()
        
        # Adaptive rate limiting
        self.error_count = 0
        self.success_count = 0
        self.adaptive_multiplier = 1.0
        
        # Timeout protection
        self.max_wait_time = 60.0  # Maximum wait time in seconds
    
    async def acquire(self, identifier: str = "global") -> bool:
        """Acquire rate limit permission"""
        async with self._lock:
            now = time.time()
            
            # Get or create bucket
            if identifier not in self.buckets:
                self.buckets[identifier] = RateLimitBucket()
            
            bucket = self.buckets[identifier]
            
            # Reset bucket if window expired
            if now >= bucket.reset_time:
                bucket.requests = 0
                bucket.reset_time = now + bucket.window_seconds
            
            # Check per-minute limit
            if self.requests_per_minute:
                limit = max(1, int(self.requests_per_minute * self.adaptive_multiplier))
                if bucket.requests >= limit:
                    return False
            
            # Check per-second limit (more granular)
            if self.requests_per_second:
                window_1s = now - (now % 1)  # Current second window
                second_bucket_key = f"{identifier}_1s_{window_1s}"
                
                if second_bucket_key not in self.buckets:
                    self.buckets[second_bucket_key] = RateLimitBucket(window_seconds=1)
                
                second_bucket = self.buckets[second_bucket_key]
                limit = max(1, int(self.requests_per_second * self.adaptive_multiplier))
                if second_bucket.requests >= limit:
                    return False
                
                second_bucket.requests += 1
            
            # Increment main bucket
            bucket.requests += 1
            return True
    
    async def wait_if_needed(self, identifier: str = "global") -> bool:
        """Wait until rate limit allows request with timeout protection"""
        start_time = time.time()
        attempts = 0
        max_attempts = 100  # Additional protection against infinite loops
        
        while not await self.acquire(identifier):
            attempts += 1
            current_time = time.time()
            
            # Check timeout
            if current_time - start_time > self.max_wait_time:
                raise TimeoutError(
                    f"Rate limiter timeout for {identifier} after {self.max_wait_time} seconds. "
                    f"Attempted {attempts} times. Consider adjusting rate limits."
                )
            
            # Check max attempts
            if attempts > max_attempts:
                raise RuntimeError(
                    f"Rate limiter exceeded max attempts ({max_attempts}) for {identifier}. "
                    f"This may indicate a configuration issue."
                )
            
            # Exponential backoff with jitter
            wait_time = min(1.0, 0.1 * (1.1 ** min(attempts, 20)))
            await asyncio.sleep(wait_time)
        
        return True
    
    async def wait_if_needed_safe(self, identifier: str = "global") -> bool:
        """Safe version that returns False instead of raising timeout exception"""
        try:
            return await self.wait_if_needed(identifier)
        except (TimeoutError, RuntimeError):
            return False
    
    def record_success(self):
        """Record successful request for adaptive rate limiting"""
        self.success_count += 1
        
        # Gradually increase rate if many successes
        if self.success_count > 50 and self.error_count < 3:
            self.adaptive_multiplier = min(1.2, self.adaptive_multiplier + 0.02)
            self.success_count = 0
            self.error_count = 0
    
    def record_error(self):
        """Record failed request for adaptive rate limiting"""
        self.error_count += 1
        
        # Decrease rate if many errors
        if self.error_count > 2:
            self.adaptive_multiplier = max(0.3, self.adaptive_multiplier - 0.2)
            self.success_count = 0
            self.error_count = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics"""
        return {
            "name": self.name,
            "requests_per_second": self.requests_per_second,
            "requests_per_minute": self.requests_per_minute,
            "adaptive_multiplier": self.adaptive_multiplier,
            "active_buckets": len(self.buckets),
            "success_count": self.success_count,
            "error_count": self.error_count,
            "max_wait_time": self.max_wait_time
        }
    
    def reset_stats(self):
        """Reset statistics (useful for testing or maintenance)"""
        self.error_count = 0
        self.success_count = 0
        self.adaptive_multiplier = 1.0
    
    def clear_old_buckets(self, max_age_seconds: int = 3600):
        """Clear old buckets to prevent memory leaks"""
        now = time.time()
        old_buckets = [
            identifier for identifier, bucket in self.buckets.items()
            if bucket.reset_time < now - max_age_seconds
        ]
        
        for identifier in old_buckets:
            del self.buckets[identifier]
        
        return len(old_buckets)