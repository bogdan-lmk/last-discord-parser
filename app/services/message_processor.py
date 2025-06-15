# app/services/message_processor.py
import asyncio
import hashlib
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
import structlog

from ..models.message import DiscordMessage
from ..models.server import SystemStats, ServerStatus
from ..config import Settings
from .discord_service import DiscordService
from .telegram_service import TelegramService

class MessageProcessor:
    """Main orchestrator with real-time message synchronization"""
    
    def __init__(self,
                 settings: Settings,
                 discord_service: DiscordService,
                 telegram_service: TelegramService,
                 redis_client = None,
                 logger = None):
        self.settings = settings
        self.discord_service = discord_service
        self.telegram_service = telegram_service
        self.logger = logger or structlog.get_logger(__name__)
        
        # State management
        self.running = False
        self.start_time = datetime.now()
        
        # Statistics
        self.stats = SystemStats()
        
        # Background tasks
        self.tasks: List[asyncio.Task] = []
        
        # Real-time message processing
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.batch_queue: List[DiscordMessage] = []
        self.redis_client = redis_client
        self.message_ttl = settings.message_ttl_seconds
        
        # Real-time synchronization state
        self.realtime_enabled = True
        self.last_sync_times: Dict[str, datetime] = {}  # server_name -> last_sync
        self.sync_intervals: Dict[str, int] = {}  # server_name -> interval_seconds
        
        # Message deduplication and rate limiting
        self.processed_message_hashes: Set[str] = set()
        self.message_rate_tracker: Dict[str, List[datetime]] = {}  # server -> timestamps
        
        # Periodic cleanup
        self.last_cleanup = datetime.now()
        
    async def initialize(self) -> bool:
        """Initialize all services and set up real-time integration"""
        self.logger.info("Initializing Message Processor with real-time sync")
        
        # Initialize Discord service
        if not await self.discord_service.initialize():
            self.logger.error("Discord service initialization failed")
            return False
        
        # Initialize Telegram service
        if not await self.telegram_service.initialize():
            self.logger.error("Telegram service initialization failed")
            return False
        
        # Set up real-time message callback
        self.discord_service.add_message_callback(self._handle_realtime_message)
        
        # Initialize sync intervals for each server
        for server_name in self.discord_service.servers.keys():
            self.sync_intervals[server_name] = 300  # 5 minutes default
            self.last_sync_times[server_name] = datetime.now()
        
        # Update initial statistics
        await self._update_stats()
        
        self.logger.info("Message Processor initialized successfully",
                        discord_servers=len(self.discord_service.servers),
                        telegram_topics=len(self.telegram_service.server_topics),
                        realtime_enabled=self.realtime_enabled)
        
        return True
    
    async def _handle_realtime_message(self, message: DiscordMessage) -> None:
        """Handle real-time message from Discord WebSocket"""
        try:
            # Create message hash for deduplication
            message_hash = self._create_message_hash(message)
            
            # Check for duplicates
            if message_hash in self.processed_message_hashes:
                self.logger.debug("Duplicate real-time message ignored", 
                                message_hash=message_hash[:8])
                return
            
            # Rate limiting check
            if not self._check_rate_limit(message.server_name):
                self.logger.warning("Rate limit exceeded for server", 
                                  server=message.server_name)
                return
            
            # Add to processing queue
            try:
                await asyncio.wait_for(
                    self.message_queue.put(message), 
                    timeout=1.0
                )
                
                # Mark as processed
                self.processed_message_hashes.add(message_hash)
                
                # Update rate tracking
                self._update_rate_tracking(message.server_name)
                
                self.logger.info("Real-time message queued", 
                               server=message.server_name,
                               channel=message.channel_name,
                               author=message.author,
                               queue_size=self.message_queue.qsize())
                
            except asyncio.TimeoutError:
                self.logger.error("Message queue timeout - dropping message",
                                server=message.server_name,
                                queue_size=self.message_queue.qsize())
                self.stats.errors_last_hour += 1
                
        except Exception as e:
            self.logger.error("Error handling real-time message",
                            server=message.server_name,
                            error=str(e))
            self.stats.errors_last_hour += 1
    
    def _create_message_hash(self, message: DiscordMessage) -> str:
        """Create unique hash for message deduplication"""
        hash_input = f"{message.guild_id}:{message.channel_id}:{message.message_id}:{message.timestamp}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def _check_rate_limit(self, server_name: str) -> bool:
        """Check if server is within rate limits"""
        now = datetime.now()
        window_start = now - timedelta(minutes=1)
        
        if server_name not in self.message_rate_tracker:
            self.message_rate_tracker[server_name] = []
        
        # Clean old timestamps
        self.message_rate_tracker[server_name] = [
            ts for ts in self.message_rate_tracker[server_name]
            if ts > window_start
        ]
        
        # Check rate (max 30 messages per minute per server)
        return len(self.message_rate_tracker[server_name]) < 30
    
    def _update_rate_tracking(self, server_name: str) -> None:
        """Update rate tracking for server"""
        if server_name not in self.message_rate_tracker:
            self.message_rate_tracker[server_name] = []
        
        self.message_rate_tracker[server_name].append(datetime.now())
    
    async def start(self) -> None:
        """Start the message processor and all background tasks"""
        if self.running:
            self.logger.warning("Message processor is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        self.logger.info("Starting Message Processor with real-time sync")
        
        # Start background tasks
        self.tasks = [
            asyncio.create_task(self._realtime_message_processor_loop()),
            asyncio.create_task(self._batch_processor_loop()),
            asyncio.create_task(self._periodic_sync_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._stats_update_loop()),
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._rate_limit_cleanup_loop())
        ]
        
        # Start Discord WebSocket monitoring (real-time)
        discord_task = asyncio.create_task(self.discord_service.start_websocket_monitoring())
        self.tasks.append(discord_task)
        
        # Start Telegram bot asynchronously
        telegram_task = asyncio.create_task(self.telegram_service.start_bot_async())
        self.tasks.append(telegram_task)
        
        # Perform initial sync
        await self._perform_initial_sync()
        
        self.logger.info("Message Processor started successfully with real-time monitoring")
        
        try:
            # Wait for all tasks with error isolation
            await asyncio.gather(*self.tasks, return_exceptions=True)
            
            # Log any individual task failures
            for task in self.tasks:
                if task.done() and task.exception():
                    self.logger.error("Task failed",
                                   task_name=task.get_name(),
                                   error=str(task.exception()))
        except Exception as e:
            self.logger.error("Critical error in message processor", error=str(e))
        finally:
            await self.stop()
    
    async def stop(self) -> None:
        """Stop the message processor and clean up"""
        if not self.running:
            return
        
        self.running = False
        self.logger.info("Stopping Message Processor")
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Cleanup services
        await self.discord_service.cleanup()
        await self.telegram_service.cleanup()
        
        self.logger.info("Message Processor stopped")
    
    async def _perform_initial_sync(self) -> None:
        """Perform initial synchronization of recent messages"""
        self.logger.info("Starting initial synchronization")
        
        total_messages = 0
        
        for server_name, server_info in self.discord_service.servers.items():
            if server_info.status != ServerStatus.ACTIVE:
                continue
            
            server_messages = []
            
            # Get recent messages from each accessible channel
            for channel_id, channel_info in server_info.accessible_channels.items():
                try:
                    messages = await self.discord_service.get_recent_messages(
                        server_name,
                        channel_id,
                        limit=min(10, self.settings.max_history_messages // len(server_info.accessible_channels))
                    )
                    
                    # Filter out duplicates
                    for msg in messages:
                        msg_hash = self._create_message_hash(msg)
                        if msg_hash not in self.processed_message_hashes:
                            server_messages.append(msg)
                            self.processed_message_hashes.add(msg_hash)
                    
                except Exception as e:
                    self.logger.error("Error getting messages during initial sync",
                                    server=server_name,
                                    channel_id=channel_id,
                                    error=str(e))
            
            if server_messages:
                # Sort by timestamp and send to Telegram
                server_messages.sort(key=lambda x: x.timestamp)
                sent_count = await self.telegram_service.send_messages_batch(server_messages)
                
                total_messages += sent_count
                self.last_sync_times[server_name] = datetime.now()
                
                self.logger.info("Initial sync for server complete",
                               server=server_name,
                               messages_sent=sent_count)
        
        self.stats.messages_processed_total += total_messages
        
        self.logger.info("Initial synchronization complete",
                        total_messages=total_messages,
                        servers_synced=len([s for s in self.discord_service.servers.values() 
                                          if s.status == ServerStatus.ACTIVE]))
    
    async def _realtime_message_processor_loop(self) -> None:
        """Real-time message processing loop"""
        self.logger.info("Starting real-time message processor loop")
        
        while self.running:
            try:
                # Get message from queue with timeout
                try:
                    message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Process message immediately for real-time sync
                await self._process_realtime_message(message)
                
                # Mark task as done
                self.message_queue.task_done()
                
            except Exception as e:
                self.logger.error("Error in real-time message processor loop", error=str(e))
                await asyncio.sleep(1)
    
    async def _process_realtime_message(self, message: DiscordMessage) -> None:
        """Process a real-time Discord message"""
        try:
            # Send to Telegram immediately
            success = await self.telegram_service.send_message(message)
            
            if success:
                self.stats.messages_processed_today += 1
                self.stats.messages_processed_total += 1
                
                # Cache in Redis if available
                if self.redis_client:
                    await self._cache_message_in_redis(message)
                
                self.logger.info("Real-time message processed successfully",
                               server=message.server_name,
                               channel=message.channel_name,
                               total_today=self.stats.messages_processed_today)
            else:
                self.stats.errors_last_hour += 1
                self.stats.last_error = "Failed to send real-time message to Telegram"
                self.stats.last_error_time = datetime.now()
                
                self.logger.error("Failed to process real-time message",
                                server=message.server_name,
                                channel=message.channel_name)
            
        except Exception as e:
            self.logger.error("Error processing real-time message", 
                            server=message.server_name,
                            error=str(e))
            
            self.stats.errors_last_hour += 1
            self.stats.last_error = str(e)
            self.stats.last_error_time = datetime.now()
    
    async def _cache_message_in_redis(self, message: DiscordMessage) -> None:
        """Cache message in Redis for deduplication"""
        if not self.redis_client:
            return
        
        try:
            message_hash = self._create_message_hash(message)
            await self.redis_client.setex(
                f"msg:{message_hash}",
                self.message_ttl,
                message.model_dump_json()
            )
        except Exception as e:
            self.logger.error("Failed to cache message in Redis", error=str(e))
    
    async def _batch_processor_loop(self) -> None:
        """Batch message processing loop (fallback for missed messages)"""
        while self.running:
            try:
                await asyncio.sleep(10)  # Process batches every 10 seconds
                
                if self.batch_queue:
                    messages_to_process = self.batch_queue.copy()
                    self.batch_queue.clear()
                    
                    if messages_to_process:
                        sent_count = await self.telegram_service.send_messages_batch(messages_to_process)
                        
                        self.stats.messages_processed_today += sent_count
                        self.stats.messages_processed_total += sent_count
                        
                        if sent_count < len(messages_to_process):
                            failed_count = len(messages_to_process) - sent_count
                            self.stats.errors_last_hour += failed_count
                
            except Exception as e:
                self.logger.error("Error in batch processor loop", error=str(e))
                await asyncio.sleep(10)
    
    async def _periodic_sync_loop(self) -> None:
        """Periodic synchronization loop (fallback for real-time failures)"""
        while self.running:
            try:
                # Run sync every 5 minutes as fallback
                await asyncio.sleep(300)
                
                self.logger.info("Starting periodic fallback sync")
                
                # Check each server for missed messages
                for server_name, last_sync in self.last_sync_times.items():
                    try:
                        # Skip if recently synced via real-time
                        if (datetime.now() - last_sync).total_seconds() < 300:
                            continue
                        
                        await self._sync_server_fallback(server_name)
                        
                    except Exception as e:
                        self.logger.error("Error in periodic sync for server",
                                        server=server_name,
                                        error=str(e))
                
                # Refresh server discovery
                await self.discord_service._discover_servers()
                
                # Clean invalid Telegram topics
                cleaned_topics = await self.telegram_service._clean_invalid_topics()
                
                if cleaned_topics > 0:
                    self.logger.info("Cleaned invalid topics", count=cleaned_topics)
                
                await self._update_stats()
                
            except Exception as e:
                self.logger.error("Error in periodic sync loop", error=str(e))
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    async def _sync_server_fallback(self, server_name: str) -> None:
        """Fallback sync for a single server"""
        if server_name not in self.discord_service.servers:
            return
        
        server_info = self.discord_service.servers[server_name]
        if server_info.status != ServerStatus.ACTIVE:
            return
        
        fallback_messages = []
        
        # Get recent messages from accessible channels
        for channel_id, channel_info in server_info.accessible_channels.items():
            try:
                messages = await self.discord_service.get_recent_messages(
                    server_name, channel_id, limit=5
                )
                
                # Filter out already processed messages
                for msg in messages:
                    msg_hash = self._create_message_hash(msg)
                    if msg_hash not in self.processed_message_hashes:
                        fallback_messages.append(msg)
                        self.processed_message_hashes.add(msg_hash)
                
            except Exception as e:
                self.logger.error("Error in fallback sync for channel",
                                server=server_name,
                                channel_id=channel_id,
                                error=str(e))
        
        if fallback_messages:
            # Sort and send
            fallback_messages.sort(key=lambda x: x.timestamp)
            sent_count = await self.telegram_service.send_messages_batch(fallback_messages)
            
            self.logger.info("Fallback sync completed",
                           server=server_name,
                           messages_sent=sent_count,
                           total_messages=len(fallback_messages))
            
            # Update sync time
            self.last_sync_times[server_name] = datetime.now()
    
    async def _cleanup_loop(self) -> None:
        """Periodic cleanup loop"""
        while self.running:
            try:
                await asyncio.sleep(self.settings.cleanup_interval_minutes * 60)
                
                # Memory cleanup
                import gc
                gc.collect()
                
                # Clean old processed message hashes
                if len(self.processed_message_hashes) > 50000:
                    # Keep only recent hashes (simple cleanup)
                    old_count = len(self.processed_message_hashes)
                    # Convert to list, sort, and keep last 25000
                    hash_list = list(self.processed_message_hashes)
                    self.processed_message_hashes = set(hash_list[-25000:])
                    
                    self.logger.info("Cleaned old message hashes",
                                   removed_count=old_count - len(self.processed_message_hashes),
                                   remaining_count=len(self.processed_message_hashes))
                
                # Reset daily stats at midnight
                now = datetime.now()
                if now.date() > self.last_cleanup.date():
                    self.stats.messages_processed_today = 0
                    self.stats.errors_last_hour = 0
                
                self.last_cleanup = now
                
                self.logger.info("Cleanup completed")
                
            except Exception as e:
                self.logger.error("Error in cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _rate_limit_cleanup_loop(self) -> None:
        """Clean up old rate tracking data"""
        while self.running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                cutoff_time = datetime.now() - timedelta(minutes=5)
                
                for server_name in list(self.message_rate_tracker.keys()):
                    # Clean old timestamps
                    old_count = len(self.message_rate_tracker[server_name])
                    self.message_rate_tracker[server_name] = [
                        ts for ts in self.message_rate_tracker[server_name]
                        if ts > cutoff_time
                    ]
                    
                    new_count = len(self.message_rate_tracker[server_name])
                    
                    if old_count != new_count:
                        self.logger.debug("Cleaned rate tracking data",
                                        server=server_name,
                                        removed_count=old_count - new_count)
                
            except Exception as e:
                self.logger.error("Error in rate limit cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _stats_update_loop(self) -> None:
        """Statistics update loop"""
        while self.running:
            try:
                await asyncio.sleep(60)  # Update stats every minute
                await self._update_stats()
                
            except Exception as e:
                self.logger.error("Error in stats update loop", error=str(e))
                await asyncio.sleep(60)
    
    async def _health_check_loop(self) -> None:
        """Health check loop"""
        while self.running:
            try:
                await asyncio.sleep(self.settings.health_check_interval)
                
                # Check Discord service health
                discord_healthy = len(self.discord_service.sessions) > 0
                discord_websockets = len(self.discord_service.websocket_connections)
                
                # Check Telegram service health  
                telegram_healthy = self.telegram_service.bot_running
                
                # Check queue sizes
                queue_healthy = self.message_queue.qsize() < 500
                
                # Check real-time processing
                realtime_healthy = discord_websockets > 0
                
                if not (discord_healthy and telegram_healthy and queue_healthy and realtime_healthy):
                    self.logger.warning("Health check failed",
                                      discord_healthy=discord_healthy,
                                      discord_websockets=discord_websockets,
                                      telegram_healthy=telegram_healthy,
                                      queue_healthy=queue_healthy,
                                      realtime_healthy=realtime_healthy,
                                      queue_size=self.message_queue.qsize())
                else:
                    self.logger.debug("Health check passed",
                                    websockets=discord_websockets,
                                    queue_size=self.message_queue.qsize())
                
            except Exception as e:
                self.logger.error("Error in health check loop", error=str(e))
                await asyncio.sleep(60)
    
    async def _update_stats(self) -> None:
        """Update system statistics"""
        try:
            # Discord stats
            discord_stats = self.discord_service.get_server_stats()
            
            self.stats.total_servers = discord_stats['total_servers']
            self.stats.active_servers = discord_stats['active_servers']
            self.stats.total_channels = discord_stats['total_channels']
            self.stats.active_channels = discord_stats['accessible_channels']
            
            # Memory usage
            import psutil
            process = psutil.Process()
            self.stats.memory_usage_mb = process.memory_info().rss / 1024 / 1024
            
            # Uptime
            self.stats.uptime_seconds = int((datetime.now() - self.start_time).total_seconds())
            
            # Rate limiting stats
            self.stats.discord_requests_per_hour = getattr(
                self.discord_service.rate_limiter, 'requests_last_hour', 0
            )
            self.stats.telegram_requests_per_hour = getattr(
                self.telegram_service.rate_limiter, 'requests_last_hour', 0
            )
            
        except Exception as e:
            self.logger.error("Error updating stats", error=str(e))
    
    def get_status(self) -> Dict[str, any]:
        """Get comprehensive system status"""
        discord_stats = self.discord_service.get_server_stats()
        
        return {
            "system": {
                "running": self.running,
                "uptime_seconds": self.stats.uptime_seconds,
                "memory_usage_mb": self.stats.memory_usage_mb,
                "health_score": self.stats.health_score,
                "status": self.stats.status,
                "realtime_enabled": self.realtime_enabled
            },
            "discord": {
                **discord_stats,
                "websocket_connections": len(self.discord_service.websocket_connections),
                "monitored_channels": len(self.discord_service.monitored_channels)
            },
            "telegram": {
                "topics": len(self.telegram_service.server_topics),
                "bot_running": self.telegram_service.bot_running,
                "messages_tracked": len(self.telegram_service.message_mappings)
            },
            "processing": {
                "queue_size": self.message_queue.qsize(),
                "batch_size": len(self.batch_queue),
                "messages_today": self.stats.messages_processed_today,
                "messages_total": self.stats.messages_processed_total,
                "errors_last_hour": self.stats.errors_last_hour,
                "last_error": self.stats.last_error,
                "last_error_time": self.stats.last_error_time.isoformat() if self.stats.last_error_time else None,
                "processed_hashes": len(self.processed_message_hashes),
                "rate_tracking_servers": len(self.message_rate_tracker)
            },
            "rate_limiting": {
                "discord": self.discord_service.rate_limiter.get_stats(),
                "telegram": self.telegram_service.rate_limiter.get_stats()
            },
            "realtime": {
                "enabled": self.realtime_enabled,
                "websocket_connections": len(self.discord_service.websocket_connections),
                "callback_count": len(self.discord_service.message_callbacks),
                "last_sync_times": {
                    server: time.isoformat() 
                    for server, time in self.last_sync_times.items()
                }
            }
        }