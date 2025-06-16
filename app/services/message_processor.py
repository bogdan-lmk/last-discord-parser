# app/services/message_processor.py - ИСПРАВЛЕННАЯ ВЕРСИЯ
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
    """ИСПРАВЛЕННЫЙ главный оркестратор с правильной синхронизацией"""
    
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
        
        # ИСПРАВЛЕНИЕ: Улучшенная дедупликация сообщений
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.batch_queue: List[DiscordMessage] = []
        self.redis_client = redis_client
        self.message_ttl = settings.message_ttl_seconds
        
        # ИСПРАВЛЕНИЕ: Глобальная дедупликация с временными окнами
        self.processed_message_hashes: Dict[str, datetime] = {}  # hash -> timestamp
        self.message_dedup_lock = asyncio.Lock()
        self.dedup_window_hours = 2  # Окно дедупликации 2 часа
        
        # Real-time synchronization state
        self.realtime_enabled = True
        self.last_sync_times: Dict[str, datetime] = {}  # server_name -> last_sync
        self.sync_intervals: Dict[str, int] = {}  # server_name -> interval_seconds
        
        # ИСПРАВЛЕНИЕ: Более точная статистика по серверам
        self.server_message_counts: Dict[str, int] = {}  # server_name -> count
        self.server_last_activity: Dict[str, datetime] = {}  # server_name -> timestamp
        
        # Message rate tracking (более точное)
        self.message_rate_tracker: Dict[str, List[datetime]] = {}  # server -> timestamps
        
        # Periodic cleanup
        self.last_cleanup = datetime.now()
        
    async def initialize(self) -> bool:
        """Initialize all services and set up real-time integration"""
        self.logger.info("Initializing ИСПРАВЛЕННЫЙ Message Processor")
        
        # Initialize Discord service
        if not await self.discord_service.initialize():
            self.logger.error("Discord service initialization failed")
            return False
        
        # Initialize Telegram service
        if not await self.telegram_service.initialize():
            self.logger.error("Telegram service initialization failed")
            return False
        
        # ENHANCED: Set Discord service reference for channel management
        self.telegram_service.set_discord_service(self.discord_service)
        
        # ИСПРАВЛЕНИЕ: Правильная регистрация колбэка
        self.discord_service.add_message_callback(self._handle_realtime_message)
        
        
        # Initialize sync intervals for each server
        for server_name in self.discord_service.servers.keys():
            self.sync_intervals[server_name] = 300  # 5 minutes default
            self.last_sync_times[server_name] = datetime.now()
            self.server_message_counts[server_name] = 0
            self.server_last_activity[server_name] = datetime.now()
        
        # Update initial statistics
        await self._update_stats()
        
        self.logger.info("ИСПРАВЛЕННЫЙ Message Processor initialized successfully",
                        discord_servers=len(self.discord_service.servers),
                        telegram_topics=len(self.telegram_service.server_topics),
                        realtime_enabled=self.realtime_enabled,
                        monitored_announcement_channels=sum(
                            s.accessible_channel_count for s in self.discord_service.servers.values()
                        ))
        
        return True
    
    async def _handle_realtime_message(self, message: DiscordMessage) -> None:
        """ИСПРАВЛЕНО: Handle real-time message from Discord"""
        try:
            # ИСПРАВЛЕНИЕ: Улучшенная дедупликация с временными окнами
            message_hash = self._create_enhanced_message_hash(message)
            
            # Проверка дублирования с блокировкой
            async with self.message_dedup_lock:
                # Очистка старых хэшей
                await self._clean_old_message_hashes()
                
                # Проверка дублей
                if message_hash in self.processed_message_hashes:
                    time_since = datetime.now() - self.processed_message_hashes[message_hash]
                    if time_since.total_seconds() < (self.dedup_window_hours * 3600):
                        self.logger.debug("Duplicate real-time message ignored", 
                                        message_hash=message_hash[:8],
                                        server=message.server_name,
                                        time_since_seconds=time_since.total_seconds())
                        return
                
                # Отмечаем как обработанное
                self.processed_message_hashes[message_hash] = datetime.now()
            
            # Rate limiting check (более мягкий для real-time)
            if not self._check_rate_limit(message.server_name, is_realtime=True):
                self.logger.warning("Rate limit exceeded for server", 
                                  server=message.server_name,
                                  is_realtime=True)
                return
            
            # ИСПРАВЛЕНИЕ: Проверяем что это announcement канал
            if not self._is_announcement_channel(message.channel_name):
                self.logger.debug("Message from non-announcement channel ignored",
                                server=message.server_name,
                                channel=message.channel_name)
                return
            
            # Add to processing queue
            try:
                await asyncio.wait_for(
                    self.message_queue.put(message), 
                    timeout=2.0
                )
                
                # Update tracking
                self._update_rate_tracking(message.server_name)
                self.server_last_activity[message.server_name] = datetime.now()
                
                self.logger.info("Real-time message queued", 
                               server=message.server_name,
                               channel=message.channel_name,
                               author=message.author,
                               queue_size=self.message_queue.qsize(),
                               message_hash=message_hash[:8])
                
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
    
    def _create_enhanced_message_hash(self, message: DiscordMessage) -> str:
        """ИСПРАВЛЕНО: Create enhanced hash for better deduplication"""
        # Включаем больше полей для точной дедупликации
        hash_input = (
            f"{message.guild_id}:"
            f"{message.channel_id}:"
            f"{message.message_id}:"
            f"{message.timestamp.isoformat()}:"
            f"{message.author}:"
            f"{hashlib.md5(message.content.encode()).hexdigest()[:8]}"
        )
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    async def _clean_old_message_hashes(self) -> None:
        """Очистка старых хэшей сообщений"""
        cutoff_time = datetime.now() - timedelta(hours=self.dedup_window_hours)
        
        old_hashes = [
            msg_hash for msg_hash, timestamp in self.processed_message_hashes.items()
            if timestamp < cutoff_time
        ]
        
        for msg_hash in old_hashes:
            del self.processed_message_hashes[msg_hash]
        
        if old_hashes:
            self.logger.debug("Cleaned old message hashes", 
                            cleaned_count=len(old_hashes),
                            remaining_count=len(self.processed_message_hashes))
    
    def _is_announcement_channel(self, channel_name: str) -> bool:
        """ИСПРАВЛЕНО: Проверка что канал является announcement"""
        channel_lower = channel_name.lower()
        announcement_keywords = ['announcement', 'announcements', 'announce']
        
        return any(keyword in channel_lower for keyword in announcement_keywords)
    
    def _check_rate_limit(self, server_name: str, is_realtime: bool = False) -> bool:
        """ИСПРАВЛЕНО: Check if server is within rate limits"""
        now = datetime.now()
        window_start = now - timedelta(minutes=1)
        
        if server_name not in self.message_rate_tracker:
            self.message_rate_tracker[server_name] = []
        
        # Clean old timestamps
        self.message_rate_tracker[server_name] = [
            ts for ts in self.message_rate_tracker[server_name]
            if ts > window_start
        ]
        
        # ИСПРАВЛЕНИЕ: Более мягкие лимиты для real-time сообщений
        limit = 60 if is_realtime else 30  # 60 для real-time, 30 для batch
        
        return len(self.message_rate_tracker[server_name]) < limit
    
    def _update_rate_tracking(self, server_name: str) -> None:
        """Update rate tracking for server"""
        if server_name not in self.message_rate_tracker:
            self.message_rate_tracker[server_name] = []
        
        self.message_rate_tracker[server_name].append(datetime.now())
        
        # Увеличиваем счетчик сообщений сервера
        if server_name not in self.server_message_counts:
            self.server_message_counts[server_name] = 0
        self.server_message_counts[server_name] += 1
    
    async def start(self) -> None:
        """Start the message processor and all background tasks"""
        if self.running:
            self.logger.warning("Message processor is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        self.logger.info("Starting ИСПРАВЛЕННЫЙ Message Processor with enhanced deduplication")
        
        # Start background tasks
        self.tasks = [
            asyncio.create_task(self._realtime_message_processor_loop()),
            asyncio.create_task(self._batch_processor_loop()),
            asyncio.create_task(self._periodic_sync_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._stats_update_loop()),
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._rate_limit_cleanup_loop()),
            asyncio.create_task(self._deduplication_cleanup_loop())  # Новый таск
        ]
        
        # Start Discord monitoring (HTTP polling for announcement channels)
        discord_task = asyncio.create_task(self.discord_service.start_websocket_monitoring())
        self.tasks.append(discord_task)
        
        # Start Telegram bot asynchronously
        telegram_task = asyncio.create_task(self.telegram_service.start_bot_async())
        self.tasks.append(telegram_task)
        
        # Perform initial sync (только announcement каналы)
        await self._perform_initial_sync()
        
        self.logger.info("ИСПРАВЛЕННЫЙ Message Processor started successfully")
        
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
        self.logger.info("Stopping ИСПРАВЛЕННЫЙ Message Processor")
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Cleanup services
        await self.discord_service.cleanup()
        await self.telegram_service.cleanup()
        
        self.logger.info("ИСПРАВЛЕННЫЙ Message Processor stopped")
    
    async def _perform_initial_sync(self) -> None:
        """ИСПРАВЛЕНО: Perform initial synchronization of recent messages"""
        self.logger.info("Starting initial synchronization (announcement channels only)")
        
        total_messages = 0
        
        for server_name, server_info in self.discord_service.servers.items():
            if server_info.status != ServerStatus.ACTIVE:
                continue
            
            server_messages = []
            
            # ИСПРАВЛЕНИЕ: Get messages only from announcement channels
            for channel_id, channel_info in server_info.accessible_channels.items():
                # Проверяем что это announcement канал
                if not self._is_announcement_channel(channel_info.channel_name):
                    self.logger.debug("Skipping non-announcement channel", 
                                    server=server_name,
                                    channel=channel_info.channel_name)
                    continue
                
                try:
                    messages = await self.discord_service.get_recent_messages(
                        server_name,
                        channel_id,
                        limit=min(5, self.settings.max_history_messages // max(1, len(server_info.accessible_channels)))
                    )
                    
                    # Filter out duplicates using enhanced hash
                    for msg in messages:
                        msg_hash = self._create_enhanced_message_hash(msg)
                        
                        async with self.message_dedup_lock:
                            if msg_hash not in self.processed_message_hashes:
                                server_messages.append(msg)
                                self.processed_message_hashes[msg_hash] = datetime.now()
                            else:
                                self.logger.debug("Duplicate message skipped in initial sync",
                                                message_hash=msg_hash[:8])
                    
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
                self.server_message_counts[server_name] = sent_count
                
                self.logger.info("Initial sync for server complete",
                               server=server_name,
                               messages_sent=sent_count,
                               announcement_channels=len([
                                   ch for ch in server_info.accessible_channels.values()
                                   if self._is_announcement_channel(ch.channel_name)
                               ]))
        
        self.stats.messages_processed_total += total_messages
        
        self.logger.info("Initial synchronization complete",
                        total_messages=total_messages,
                        servers_synced=len([s for s in self.discord_service.servers.values() 
                                          if s.status == ServerStatus.ACTIVE]),
                        announcement_channels_only=True)
    
    async def _realtime_message_processor_loop(self) -> None:
        """Real-time message processing loop"""
        self.logger.info("Starting real-time message processor loop (announcement channels)")
        
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
        """ИСПРАВЛЕНО: Process a real-time Discord message"""
        try:
            # Double-check это announcement канал
            if not self._is_announcement_channel(message.channel_name):
                self.logger.debug("Non-announcement message skipped in processing",
                                server=message.server_name,
                                channel=message.channel_name)
                return
            
            # Send to Telegram immediately
            success = await self.telegram_service.send_message(message)
            
            if success:
                self.stats.messages_processed_today += 1
                self.stats.messages_processed_total += 1
                self.server_message_counts[message.server_name] += 1
                
                # Cache in Redis if available
                if self.redis_client:
                    await self._cache_message_in_redis(message)
                
                self.logger.info("Real-time announcement message processed",
                               server=message.server_name,
                               channel=message.channel_name,
                               total_today=self.stats.messages_processed_today,
                               server_total=self.server_message_counts[message.server_name])
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
            message_hash = self._create_enhanced_message_hash(message)
            await self.redis_client.setex(
                f"msg:{message_hash}",
                self.message_ttl,
                message.model_dump_json()
            )
        except Exception as e:
            self.logger.error("Failed to cache message in Redis", error=str(e))
    
    async def _deduplication_cleanup_loop(self) -> None:
        """НОВЫЙ: Periodic cleanup of deduplication data"""
        while self.running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                async with self.message_dedup_lock:
                    await self._clean_old_message_hashes()
                
                # Clean rate tracking data
                cutoff_time = datetime.now() - timedelta(minutes=10)
                
                for server_name in list(self.message_rate_tracker.keys()):
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
                
                self.logger.debug("Deduplication cleanup completed",
                                processed_hashes=len(self.processed_message_hashes))
                
            except Exception as e:
                self.logger.error("Error in deduplication cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _batch_processor_loop(self) -> None:
        """Batch message processing loop (fallback for missed messages)"""
        while self.running:
            try:
                await asyncio.sleep(10)  # Process batches every 10 seconds
                
                if self.batch_queue:
                    messages_to_process = self.batch_queue.copy()
                    self.batch_queue.clear()
                    
                    # ИСПРАВЛЕНИЕ: Filter only announcement channel messages
                    announcement_messages = [
                        msg for msg in messages_to_process
                        if self._is_announcement_channel(msg.channel_name)
                    ]
                    
                    if announcement_messages:
                        sent_count = await self.telegram_service.send_messages_batch(announcement_messages)
                        
                        self.stats.messages_processed_today += sent_count
                        self.stats.messages_processed_total += sent_count
                        
                        if sent_count < len(announcement_messages):
                            failed_count = len(announcement_messages) - sent_count
                            self.stats.errors_last_hour += failed_count
                        
                        self.logger.info("Batch processed (announcement only)",
                                       processed=sent_count,
                                       total_submitted=len(messages_to_process),
                                       announcement_only=len(announcement_messages))
                
            except Exception as e:
                self.logger.error("Error in batch processor loop", error=str(e))
                await asyncio.sleep(10)
    
    async def _periodic_sync_loop(self) -> None:
        """Periodic synchronization loop (fallback for real-time failures)"""
        while self.running:
            try:
                # Run sync every 5 minutes as fallback
                await asyncio.sleep(300)
                
                self.logger.info("Starting periodic fallback sync (announcement channels)")
                
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
                await self.discord_service._discover_servers_with_retry()
                
                # Clean invalid Telegram topics
                cleaned_topics = await self.telegram_service._clean_invalid_topics()
                
                if cleaned_topics > 0:
                    self.logger.info("Cleaned invalid topics", count=cleaned_topics)
                
                await self._update_stats()
                
            except Exception as e:
                self.logger.error("Error in periodic sync loop", error=str(e))
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    async def _sync_server_fallback(self, server_name: str) -> None:
        """ИСПРАВЛЕНО: Fallback sync for a single server (announcement channels only)"""
        if server_name not in self.discord_service.servers:
            return
        
        server_info = self.discord_service.servers[server_name]
        if server_info.status != ServerStatus.ACTIVE:
            return
        
        fallback_messages = []
        
        # ИСПРАВЛЕНИЕ: Get messages only from announcement channels
        for channel_id, channel_info in server_info.accessible_channels.items():
            # Проверяем что это announcement канал
            if not self._is_announcement_channel(channel_info.channel_name):
                continue
            
            try:
                messages = await self.discord_service.get_recent_messages(
                    server_name, channel_id, limit=3
                )
                
                # Filter out already processed messages
                for msg in messages:
                    msg_hash = self._create_enhanced_message_hash(msg)
                    
                    async with self.message_dedup_lock:
                        if msg_hash not in self.processed_message_hashes:
                            fallback_messages.append(msg)
                            self.processed_message_hashes[msg_hash] = datetime.now()
                
            except Exception as e:
                self.logger.error("Error in fallback sync for channel",
                                server=server_name,
                                channel_id=channel_id,
                                error=str(e))
        
        if fallback_messages:
            # Sort and send
            fallback_messages.sort(key=lambda x: x.timestamp)
            sent_count = await self.telegram_service.send_messages_batch(fallback_messages)
            
            self.logger.info("Fallback sync completed (announcement channels)",
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
                
                # Reset daily stats at midnight
                now = datetime.now()
                if now.date() > self.last_cleanup.date():
                    self.stats.messages_processed_today = 0
                    self.stats.errors_last_hour = 0
                    
                    # Reset daily server counters
                    for server_name in self.server_message_counts:
                        self.server_message_counts[server_name] = 0
                
                self.last_cleanup = now
                
                self.logger.info("Cleanup completed",
                               processed_hashes=len(self.processed_message_hashes))
                
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
                
                # Check Telegram service health  
                telegram_healthy = self.telegram_service.bot_running
                
                # Check queue sizes
                queue_healthy = self.message_queue.qsize() < 500
                
                # Check deduplication health
                dedup_healthy = len(self.processed_message_hashes) < 100000
                
                if not (discord_healthy and telegram_healthy and queue_healthy and dedup_healthy):
                    self.logger.warning("Health check failed",
                                      discord_healthy=discord_healthy,
                                      telegram_healthy=telegram_healthy,
                                      queue_healthy=queue_healthy,
                                      dedup_healthy=dedup_healthy,
                                      queue_size=self.message_queue.qsize(),
                                      processed_hashes=len(self.processed_message_hashes))
                else:
                    self.logger.debug("Health check passed",
                                    queue_size=self.message_queue.qsize(),
                                    processed_hashes=len(self.processed_message_hashes))
                
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
        """Get comprehensive system status with enhanced features"""
        discord_stats = self.discord_service.get_server_stats()
        
        # НОВОЕ: Добавляем enhanced статистики Telegram
        enhanced_features = {}
        try:
            enhanced_stats = self.telegram_service.get_enhanced_stats()
            enhanced_features = {
                "telegram_enhanced": enhanced_stats,
                "anti_duplicate_active": self.telegram_service.startup_verification_done,
                "bot_interface_active": self.telegram_service.bot_running,
                "user_states_count": len(getattr(self.telegram_service, 'user_states', {})),
                "processed_messages_cache": len(getattr(self.telegram_service, 'processed_messages', {}))
            }
        except Exception as e:
            enhanced_features = {"error": str(e), "available": False}
        
        # Подсчет announcement каналов
        announcement_channels = 0
        for server_info in self.discord_service.servers.values():
            for channel_info in server_info.accessible_channels.values():
                if self._is_announcement_channel(channel_info.channel_name):
                    announcement_channels += 1
        
        return {
            "system": {
                "running": self.running,
                "uptime_seconds": self.stats.uptime_seconds,
                "memory_usage_mb": self.stats.memory_usage_mb,
                "health_score": self.stats.health_score,
                "status": self.stats.status,
                "realtime_enabled": self.realtime_enabled,
                "version": "Enhanced версия с bot interface"  # ОБНОВЛЕНО
            },
            "discord": {
                **discord_stats,
                "announcement_channels": announcement_channels,
                "announcement_only": True
            },
            "telegram": {
                "topics": len(self.telegram_service.server_topics),
                "bot_running": self.telegram_service.bot_running,
                "messages_tracked": len(self.telegram_service.message_mappings),
                "one_topic_per_server": True,
                "enhanced_interface": True  # НОВОЕ
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
                "dedup_window_hours": self.dedup_window_hours
            },
            "rate_limiting": {
                "discord": self.discord_service.rate_limiter.get_stats(),
                "telegram": self.telegram_service.rate_limiter.get_stats()
            },
            "enhanced_features": enhanced_features,  # НОВОЕ
            "servers": {
                server_name: {
                    "message_count": self.server_message_counts.get(server_name, 0),
                    "last_activity": self.server_last_activity.get(server_name, datetime.now()).isoformat(),
                    "last_sync": self.last_sync_times.get(server_name, datetime.now()).isoformat(),
                    "announcement_channels": len([
                        ch for ch in self.discord_service.servers[server_name].accessible_channels.values()
                        if self._is_announcement_channel(ch.channel_name)
                    ]) if server_name in self.discord_service.servers else 0
                }
                for server_name in self.discord_service.servers.keys()
            }
        }