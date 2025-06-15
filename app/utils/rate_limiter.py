# app/utils/rate_limiter.py - DISCORD-AWARE VERSION
import asyncio
import time
import math
from typing import Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta

@dataclass
class RateLimitBucket:
    """Enhanced rate limiting bucket for Discord API"""
    requests: int = 0
    reset_time: float = 0
    window_seconds: float = 60
    
    # Discord-specific fields
    remaining: int = 0
    limit: int = 0
    retry_after: Optional[float] = None
    global_rate_limit: bool = False
    
    # Adaptive fields
    consecutive_errors: int = 0
    last_success: float = field(default_factory=time.time)
    backoff_multiplier: float = 1.0

class DiscordRateLimiter:
    """Discord-aware rate limiter with intelligent backoff and recovery"""
    
    def __init__(self, 
                 requests_per_second: Optional[float] = None,
                 requests_per_minute: Optional[int] = None,
                 name: str = "default"):
        self.name = name
        self.requests_per_second = requests_per_second or 2.0
        self.requests_per_minute = requests_per_minute or 120
        
        # Discord-specific limits
        self.global_rate_limit_reset = 0.0
        self.global_rate_limited = False
        
        # Tracking buckets with route-specific limits
        self.buckets: Dict[str, RateLimitBucket] = {}
        self._lock = asyncio.Lock()
        
        # Adaptive rate limiting
        self.error_count = 0
        self.success_count = 0
        self.adaptive_multiplier = 1.0
        
        # Enhanced timeout protection
        self.max_wait_time = 300.0  # 5 minutes max wait
        self.min_request_interval = 0.5  # Minimum 500ms between requests
        
        # Statistics
        self.total_requests = 0
        self.total_rate_limited = 0
        self.total_errors = 0
        
        # Preemptive rate limiting
        self.preemptive_slowdown = False
        self.last_429_time = 0.0
    
    async def acquire(self, identifier: str = "global", route: Optional[str] = None) -> bool:
        """Acquire rate limit permission with Discord-aware logic"""
        async with self._lock:
            now = time.time()
            
            # Check global rate limit first
            if self.global_rate_limited and now < self.global_rate_limit_reset:
                return False
            elif self.global_rate_limited and now >= self.global_rate_limit_reset:
                self.global_rate_limited = False
                self.global_rate_limit_reset = 0.0
            
            # Get or create bucket
            bucket_key = f"{identifier}:{route}" if route else identifier
            if bucket_key not in self.buckets:
                self.buckets[bucket_key] = RateLimitBucket()
            
            bucket = self.buckets[bucket_key]
            
            # Check if bucket is still rate limited
            if bucket.retry_after and now < bucket.retry_after:
                return False
            elif bucket.retry_after and now >= bucket.retry_after:
                bucket.retry_after = None
            
            # Reset bucket if window expired
            if now >= bucket.reset_time:
                bucket.requests = 0
                bucket.reset_time = now + bucket.window_seconds
                bucket.remaining = bucket.limit if bucket.limit > 0 else 100  # Default limit
            
            # Calculate effective limits with adaptive multiplier
            per_minute_limit = max(1, int(self.requests_per_minute * self.adaptive_multiplier))
            per_second_limit = max(0.1, self.requests_per_second * self.adaptive_multiplier)
            
            # Apply backoff multiplier for this specific bucket
            if bucket.consecutive_errors > 0:
                per_minute_limit = max(1, int(per_minute_limit / bucket.backoff_multiplier))
                per_second_limit = max(0.1, per_second_limit / bucket.backoff_multiplier)
            
            # Check per-minute limit
            if bucket.requests >= per_minute_limit:
                return False
            
            # Check per-second limit with more granular windows
            current_second = int(now)
            second_bucket_key = f"{bucket_key}_1s_{current_second}"
            
            if second_bucket_key not in self.buckets:
                self.buckets[second_bucket_key] = RateLimitBucket(window_seconds=1)
            
            second_bucket = self.buckets[second_bucket_key]
            if second_bucket.requests >= per_second_limit:
                return False
            
            # Check minimum interval between requests
            time_since_last = now - bucket.last_success
            if time_since_last < self.min_request_interval:
                return False
            
            # Check if we're in preemptive slowdown mode
            if self.preemptive_slowdown:
                # Implement more conservative limits
                time_since_429 = now - self.last_429_time
                if time_since_429 < 60:  # Stay conservative for 1 minute after 429
                    conservative_limit = max(1, int(per_minute_limit * 0.3))
                    if bucket.requests >= conservative_limit:
                        return False
            
            # All checks passed, increment counters
            bucket.requests += 1
            second_bucket.requests += 1
            bucket.last_success = now
            
            return True
    
    async def wait_if_needed(self, identifier: str = "global", route: Optional[str] = None) -> bool:
        """Wait until rate limit allows request with intelligent backoff"""
        start_time = time.time()
        attempts = 0
        max_attempts = 200
        
        while not await self.acquire(identifier, route):
            attempts += 1
            current_time = time.time()
            
            # Check timeout
            if current_time - start_time > self.max_wait_time:
                raise TimeoutError(
                    f"Discord rate limiter timeout for {identifier} after "
                    f"{self.max_wait_time} seconds. This usually indicates severe rate limiting."
                )
            
            # Check max attempts
            if attempts > max_attempts:
                raise RuntimeError(
                    f"Discord rate limiter exceeded max attempts ({max_attempts}) for {identifier}. "
                    f"Discord API may be experiencing issues."
                )
            
            # Calculate intelligent wait time
            wait_time = await self._calculate_wait_time(identifier, route, attempts)
            
            # Add jitter to prevent thundering herd
            jitter = 0.1 * wait_time * (0.5 - asyncio.get_event_loop().time() % 1)
            total_wait = wait_time + jitter
            
            await asyncio.sleep(total_wait)
        
        self.total_requests += 1
        return True
    
    async def _calculate_wait_time(self, identifier: str, route: Optional[str], attempts: int) -> float:
        """Calculate intelligent wait time based on current state"""
        base_wait = 0.5
        
        # Check bucket-specific state
        bucket_key = f"{identifier}:{route}" if route else identifier
        bucket = self.buckets.get(bucket_key)
        
        if bucket:
            # If we have a specific retry_after, use it
            if bucket.retry_after:
                remaining_time = bucket.retry_after - time.time()
                if remaining_time > 0:
                    return min(remaining_time + 0.1, 60.0)  # Cap at 1 minute
            
            # Calculate based on bucket state
            if bucket.consecutive_errors > 0:
                base_wait *= bucket.backoff_multiplier
        
        # Check global rate limit
        if self.global_rate_limited:
            remaining_global = self.global_rate_limit_reset - time.time()
            if remaining_global > 0:
                return min(remaining_global + 0.1, 60.0)
        
        # Exponential backoff with cap
        exponential_wait = base_wait * (1.5 ** min(attempts, 10))
        
        # If we're in preemptive slowdown, increase wait time
        if self.preemptive_slowdown:
            exponential_wait *= 2.0
        
        # Cap the wait time
        return min(exponential_wait, 30.0)
    
    def handle_rate_limit_response(self, identifier: str, headers: Dict[str, str], 
                                 route: Optional[str] = None, status_code: int = 429):
        """Handle Discord rate limit response headers"""
        bucket_key = f"{identifier}:{route}" if route else identifier
        
        if bucket_key not in self.buckets:
            self.buckets[bucket_key] = RateLimitBucket()
        
        bucket = self.buckets[bucket_key]
        now = time.time()
        
        # Parse Discord headers
        if 'x-ratelimit-limit' in headers:
            bucket.limit = int(headers['x-ratelimit-limit'])
        
        if 'x-ratelimit-remaining' in headers:
            bucket.remaining = int(headers['x-ratelimit-remaining'])
        
        if 'x-ratelimit-reset-after' in headers:
            reset_after = float(headers['x-ratelimit-reset-after'])
            bucket.reset_time = now + reset_after
        
        if status_code == 429:
            self.total_rate_limited += 1
            self.last_429_time = now
            self.preemptive_slowdown = True
            
            # Handle retry-after
            if 'retry-after' in headers:
                retry_after_seconds = float(headers['retry-after'])
                bucket.retry_after = now + retry_after_seconds
                
                # Check if this is a global rate limit
                if 'x-ratelimit-global' in headers:
                    self.global_rate_limited = True
                    self.global_rate_limit_reset = now + retry_after_seconds
            
            # Increase backoff for this bucket
            bucket.consecutive_errors += 1
            bucket.backoff_multiplier = min(4.0, 1.0 + (bucket.consecutive_errors * 0.5))
            
            # Record error
            self.record_error()
        else:
            # Successful request, reset error state
            bucket.consecutive_errors = max(0, bucket.consecutive_errors - 1)
            if bucket.consecutive_errors == 0:
                bucket.backoff_multiplier = 1.0
            
            self.record_success()
    
    def record_success(self):
        """Record successful request for adaptive rate limiting"""
        self.success_count += 1
        
        # Gradually disable preemptive slowdown after successes
        if self.success_count > 10 and self.preemptive_slowdown:
            time_since_429 = time.time() - self.last_429_time
            if time_since_429 > 120:  # 2 minutes of no rate limits
                self.preemptive_slowdown = False
        
        # Gradually increase rate if many successes
        if self.success_count > 50 and self.error_count < 3:
            self.adaptive_multiplier = min(1.2, self.adaptive_multiplier + 0.01)
            self.success_count = 0
            self.error_count = 0
    
    def record_error(self):
        """Record failed request for adaptive rate limiting"""
        self.error_count += 1
        self.total_errors += 1
        
        # Decrease rate if many errors
        if self.error_count > 3:
            self.adaptive_multiplier = max(0.2, self.adaptive_multiplier - 0.3)
            self.preemptive_slowdown = True
            self.success_count = 0
            self.error_count = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive rate limiter statistics"""
        now = time.time()
        active_buckets = len([b for b in self.buckets.values() 
                            if now - b.last_success < 300])  # Active in last 5 minutes
        
        return {
            "name": self.name,
            "requests_per_second": self.requests_per_second,
            "requests_per_minute": self.requests_per_minute,
            "adaptive_multiplier": round(self.adaptive_multiplier, 3),
            "active_buckets": active_buckets,
            "total_buckets": len(self.buckets),
            "success_count": self.success_count,
            "error_count": self.error_count,
            "total_requests": self.total_requests,
            "total_rate_limited": self.total_rate_limited,
            "total_errors": self.total_errors,
            "global_rate_limited": self.global_rate_limited,
            "preemptive_slowdown": self.preemptive_slowdown,
            "max_wait_time": self.max_wait_time,
            "success_rate": round(
                self.success_count / max(1, self.success_count + self.error_count) * 100, 2
            ),
            "rate_limit_rate": round(
                self.total_rate_limited / max(1, self.total_requests) * 100, 2
            )
        }
    
    def reset_stats(self):
        """Reset statistics (useful for testing or maintenance)"""
        self.error_count = 0
        self.success_count = 0
        self.total_requests = 0
        self.total_rate_limited = 0
        self.total_errors = 0
        self.adaptive_multiplier = 1.0
        self.preemptive_slowdown = False
    
    def clear_old_buckets(self, max_age_seconds: int = 3600):
        """Clear old buckets to prevent memory leaks"""
        now = time.time()
        old_buckets = [
            identifier for identifier, bucket in self.buckets.items()
            if (now - bucket.last_success) > max_age_seconds
        ]
        
        for identifier in old_buckets:
            del self.buckets[identifier]
        
        return len(old_buckets)
    
    def get_bucket_info(self, identifier: str, route: Optional[str] = None) -> Dict[str, Any]:
        """Get information about a specific bucket"""
        bucket_key = f"{identifier}:{route}" if route else identifier
        bucket = self.buckets.get(bucket_key)
        
        if not bucket:
            return {"exists": False}
        
        now = time.time()
        return {
            "exists": True,
            "requests": bucket.requests,
            "remaining": bucket.remaining,
            "limit": bucket.limit,
            "reset_in": max(0, bucket.reset_time - now),
            "retry_after": max(0, bucket.retry_after - now) if bucket.retry_after else 0,
            "consecutive_errors": bucket.consecutive_errors,
            "backoff_multiplier": bucket.backoff_multiplier,
            "time_since_last_success": now - bucket.last_success
        }
    
    async def wait_for_bucket_reset(self, identifier: str, route: Optional[str] = None):
        """Wait for a specific bucket to reset"""
        bucket_key = f"{identifier}:{route}" if route else identifier
        bucket = self.buckets.get(bucket_key)
        
        if not bucket:
            return
        
        now = time.time()
        wait_time = 0
        
        if bucket.retry_after and bucket.retry_after > now:
            wait_time = bucket.retry_after - now
        elif bucket.reset_time > now:
            wait_time = bucket.reset_time - now
        
        if wait_time > 0:
            await asyncio.sleep(min(wait_time + 0.1, 60.0))

# For backward compatibility, alias the original class name
RateLimiter = DiscordRateLimiter