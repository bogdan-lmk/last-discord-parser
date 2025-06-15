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
    """Main orchestrator that coordinates Discord and Telegram services"""
    
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
        
        # Message queues and deduplication
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.batch_queue: List[DiscordMessage] = []
        self.redis_client = redis_client
        self.message_ttl = settings.message_ttl_seconds
        
        # Periodic cleanup
        self.last_cleanup = datetime.now()
        
    async def initialize(self) -> bool:
        """Initialize all services and components"""
        self.logger.info("Initializing Message Processor")
        
        # Initialize Discord service
        if not await self.discord_service.initialize():
            self.logger.error("Discord service initialization failed")
            return False
        
        # Initialize Telegram service
        if not await self.telegram_service.initialize():
            self.logger.error("Telegram service initialization failed")
            return False
        
        # Update initial statistics
        await self._update_stats()
        
        self.logger.info("Message Processor initialized successfully",
                        discord_servers=len(self.discord_service.servers),
                        telegram_topics=len(self.telegram_service.server_topics))
        
        return True
    
    async def start(self) -> None:
        """Start the message processor and all background tasks"""
        if self.running:
            self.logger.warning("Message processor is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        self.logger.info("Starting Message Processor")
        
        # Start background tasks
        self.tasks = [
            asyncio.create_task(self._message_processor_loop()),
            asyncio.create_task(self._batch_processor_loop()),
            asyncio.create_task(self._periodic_sync_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._stats_update_loop()),
            asyncio.create_task(self._health_check_loop())
        ]
        
        # Start Discord WebSocket monitoring
        discord_task = asyncio.create_task(self.discord_service.start_websocket_monitoring())
        self.tasks.append(discord_task)
        
        # Start Telegram bot asynchronously
        telegram_task = asyncio.create_task(self.telegram_service.start_bot_async())
        self.tasks.append(telegram_task)
        
        # Perform initial sync
        await self._perform_initial_sync()
        
        self.logger.info("Message Processor started successfully")
        
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
                    
                    server_messages.extend(messages)
                    
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
                
                self.logger.info("Initial sync for server complete",
                               server=server_name,
                               messages_sent=sent_count)
        
        self.stats.messages_processed_total += total_messages
        
        self.logger.info("Initial synchronization complete",
                        total_messages=total_messages,
                        servers_synced=len([s for s in self.discord_service.servers.values() 
                                          if s.status == ServerStatus.ACTIVE]))
    
    async def _message_processor_loop(self) -> None:
        """Main message processing loop"""
        while self.running:
            try:
                # Get message from queue (wait up to 1 second)
                try:
                    message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Process message
                await self._process_single_message(message)
                
                # Mark task as done
                self.message_queue.task_done()
                
            except Exception as e:
                self.logger.error("Error in message processor loop", error=str(e))
                await asyncio.sleep(1)
    
    async def _process_single_message(self, message: DiscordMessage) -> None:
        """Process a single Discord message"""
        try:
            # Create unique message ID
            message_hash = hashlib.md5(
                f"{message.guild_id}:{message.channel_id}:{message.message_id}".encode()
            ).hexdigest()
            
            # Check if already processed
            if await self._is_message_processed(message_hash):
                self.logger.debug("Message already processed", message_hash=message_hash)
                return
                
            # Send to Telegram
            success = await self.telegram_service.send_message(message)
            
            if success:
                self.stats.messages_processed_today += 1
                self.stats.messages_processed_total += 1
                await self._mark_message_processed(message_hash)
            else:
                self.stats.errors_last_hour += 1
                self.stats.last_error = "Failed to send message to Telegram"
                self.stats.last_error_time = datetime.now()
            
        except Exception as e:
            self.logger.error("Error processing message", 
                            server=message.server_name,
                            error=str(e))
            
            self.stats.errors_last_hour += 1
            self.stats.last_error = str(e)
            self.stats.last_error_time = datetime.now()
    
    async def _batch_processor_loop(self) -> None:
        """Batch message processing loop"""
        while self.running:
            try:
                await asyncio.sleep(5)  # Process batches every 5 seconds
                
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
                await asyncio.sleep(5)
    
    async def _periodic_sync_loop(self) -> None:
        """Periodic synchronization loop"""
        while self.running:
            try:
                # Run sync every 30 minutes
                await asyncio.sleep(1800)
                
                self.logger.info("Starting periodic sync")
                
                # Refresh server discovery
                await self.discord_service._discover_servers()
                
                # Clean invalid Telegram topics
                cleaned_topics = await self.telegram_service._clean_invalid_topics()
                
                if cleaned_topics > 0:
                    self.logger.info("Cleaned invalid topics", count=cleaned_topics)
                
                await self._update_stats()
                
            except Exception as e:
                self.logger.error("Error in periodic sync loop", error=str(e))
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def _cleanup_loop(self) -> None:
        """Periodic cleanup loop"""
        while self.running:
            try:
                await asyncio.sleep(self.settings.cleanup_interval_minutes * 60)
                
                # Memory cleanup
                import gc
                gc.collect()
                
                # Clear old rate limiter buckets
                for service in [self.discord_service, self.telegram_service]:
                    if hasattr(service, 'rate_limiter'):
                        # Clear old buckets (older than 1 hour)
                        cutoff_time = datetime.now().timestamp() - 3600
                        old_buckets = [
                            key for key, bucket in service.rate_limiter.buckets.items()
                            if bucket.reset_time < cutoff_time
                        ]
                        for key in old_buckets:
                            del service.rate_limiter.buckets[key]
                
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
                
                # Check Telegram service health  
                telegram_healthy = self.telegram_service.bot_running
                
                # Check queue sizes
                queue_healthy = self.message_queue.qsize() < 500
                
                if not (discord_healthy and telegram_healthy and queue_healthy):
                    self.logger.warning("Health check failed",
                                      discord_healthy=discord_healthy,
                                      telegram_healthy=telegram_healthy,
                                      queue_healthy=queue_healthy,
                                      queue_size=self.message_queue.qsize())
                else:
                    self.logger.debug("Health check passed")
                
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
    
    async def queue_message(self, message: DiscordMessage) -> None:
        """Queue a message for processing"""
        try:
            await self.message_queue.put(message)
        except asyncio.QueueFull:
            self.logger.error("Message queue is full, dropping message",
                            server=message.server_name,
                            channel=message.channel_name)
            self.stats.errors_last_hour += 1
    
    def add_to_batch(self, messages: List[DiscordMessage]) -> None:
        """Add messages to batch queue"""
        self.batch_queue.extend(messages)
        
        # If batch gets too large, process immediately
        if len(self.batch_queue) >= self.settings.message_batch_size:
            asyncio.create_task(self._process_batch())
    
    async def _process_batch(self) -> None:
        """Process current batch immediately"""
        if not self.batch_queue:
            return
        
        messages_to_process = self.batch_queue.copy()
        self.batch_queue.clear()
        
        sent_count = await self.telegram_service.send_messages_batch(messages_to_process)
        
        self.stats.messages_processed_today += sent_count
        self.stats.messages_processed_total += sent_count
        
        if sent_count < len(messages_to_process):
            failed_count = len(messages_to_process) - sent_count
            self.stats.errors_last_hour += failed_count
    
    async def _is_message_processed(self, message_hash: str) -> bool:
        """Check if message was already processed"""
        if not self.redis_client:
            return False
            
        try:
            return await self.redis_client.exists(f"msg:{message_hash}")
        except Exception as e:
            self.logger.error("Redis check failed", error=str(e))
            return False

    async def _mark_message_processed(self, message_hash: str) -> None:
        """Mark message as processed"""
        if not self.redis_client:
            return
            
        try:
            await self.redis_client.setex(
                f"msg:{message_hash}",
                self.message_ttl,
                "1"
            )
        except Exception as e:
            self.logger.error("Redis mark failed", error=str(e))

    def get_status(self) -> Dict[str, any]:
        """Get comprehensive system status"""
        return {
            "system": {
                "running": self.running,
                "uptime_seconds": self.stats.uptime_seconds,
                "memory_usage_mb": self.stats.memory_usage_mb,
                "health_score": self.stats.health_score,
                "status": self.stats.status
            },
            "discord": self.discord_service.get_server_stats(),
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
                "last_error_time": self.stats.last_error_time.isoformat() if self.stats.last_error_time else None
            },
            "rate_limiting": {
                "discord": self.discord_service.rate_limiter.get_stats(),
                "telegram": self.telegram_service.rate_limiter.get_stats()
            }
        }
