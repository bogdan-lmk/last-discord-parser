# app/services/message_processor.py - ОБНОВЛЕННЫЙ для WebSocket
import asyncio
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Set
import structlog

from ..models.message import DiscordMessage
from ..models.server import SystemStats, ServerStatus
from ..config import Settings
from .discord_service import DiscordService
from .telegram_service import TelegramService

class MessageProcessor:
    """Главный оркестратор - WebSocket ONLY (БЕЗ дублирования сообщений)"""
    
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
        
        # WebSocket message queue
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.redis_client = redis_client
        
        # Channel initialization tracking
        self.last_processed_message_per_channel: Dict[str, datetime] = {}  # channel_id -> last_timestamp
        self.channel_initialization_done: Set[str] = set()  # каналы, для которых выполнена начальная инициализация
        
        # WebSocket-only дедупликация
        self.processed_message_hashes: Set[str] = set()  # Только хэши уже обработанных сообщений
        self.message_dedup_lock = asyncio.Lock()
        
        # Real-time synchronization state
        self.realtime_enabled = True
        
        # Server statistics
        self.server_message_counts: Dict[str, int] = {}  # server_name -> count
        self.server_last_activity: Dict[str, datetime] = {}  # server_name -> timestamp
        
        # Message rate tracking
        self.message_rate_tracker: Dict[str, List[datetime]] = {}  # server -> timestamps
        
        # Periodic cleanup
        self.last_cleanup = datetime.now()
        
        # Флаг завершения инициализации
        self.initial_sync_completed = False
        
        # WebSocket monitoring stats
        self.websocket_messages_received = 0
        self.websocket_messages_processed = 0
        self.websocket_last_message_time = None
        
    async def initialize(self) -> bool:
        """Initialize all services and set up WebSocket integration"""
        self.logger.info("🚀 Initializing Message Processor with WebSocket-only system")
        
        # Initialize Discord service (with WebSocket support)
        if not await self.discord_service.initialize():
            self.logger.error("❌ Discord service initialization failed")
            return False
        
        # Initialize Telegram service
        if not await self.telegram_service.initialize():
            self.logger.error("❌ Telegram service initialization failed")
            return False
        
        # Set Discord service reference for channel management
        self.telegram_service.set_discord_service(self.discord_service)
        
        # Register WebSocket message callback
        self.discord_service.add_message_callback(self._handle_websocket_message)
        
        # Initialize server tracking
        for server_name in self.discord_service.servers.keys():
            self.server_message_counts[server_name] = 0
            self.server_last_activity[server_name] = datetime.now()
        
        # Update initial statistics
        await self._update_stats()
        
        self.logger.info("✅ Message Processor initialized successfully",
                        discord_servers=len(self.discord_service.servers),
                        telegram_topics=len(self.telegram_service.server_topics),
                        realtime_enabled=self.realtime_enabled,
                        monitored_channels=len(self.discord_service.monitored_announcement_channels),
                        mode="WebSocket-only",
                        anti_duplication="ACTIVE")
        
        return True
    
    async def _handle_websocket_message(self, message: DiscordMessage) -> None:
        """Handle WebSocket message - ONLY process if newer than last seen"""
        try:
            self.websocket_messages_received += 1
            self.websocket_last_message_time = datetime.now()
            
            # Check if channel is monitored
            if message.channel_id not in self.discord_service.monitored_announcement_channels:
                self.logger.debug("🚫 Non-monitored channel message ignored",
                                server=message.server_name,
                                channel=message.channel_name)
                return
            
            # Check if channel initialized (initial sync must be complete)
            if not self.initial_sync_completed or message.channel_id not in self.channel_initialization_done:
                self.logger.debug("⏳ Channel not initialized yet, queuing message",
                                server=message.server_name,
                                channel=message.channel_name)
                
                # Queue for processing after initialization
                try:
                    await asyncio.wait_for(self.message_queue.put(message), timeout=1.0)
                except asyncio.TimeoutError:
                    self.logger.warning("⚠️ Queue timeout during initialization")
                return
            
            # Check if message is newer than last processed
            last_processed = self.last_processed_message_per_channel.get(message.channel_id)
            if last_processed and message.timestamp <= last_processed:
                self.logger.debug("🔄 Message not newer than last processed, skipping",
                                channel_id=message.channel_id,
                                message_timestamp=message.timestamp.isoformat(),
                                last_processed_timestamp=last_processed.isoformat())
                return
            
            # Deduplication check
            message_hash = self._create_message_hash(message)
            async with self.message_dedup_lock:
                if message_hash in self.processed_message_hashes:
                    self.logger.debug("🔂 Duplicate WebSocket message ignored",
                                    message_hash=message_hash[:8],
                                    server=message.server_name)
                    return
                self.processed_message_hashes.add(message_hash)
            
            # Rate limit check
            if not self._check_rate_limit(message.server_name, is_realtime=True):
                self.logger.warning("🚦 Rate limit exceeded for WebSocket message", 
                                  server=message.server_name)
                return
            
            # Queue the message for processing
            try:
                await asyncio.wait_for(self.message_queue.put(message), timeout=2.0)
                self.last_processed_message_per_channel[message.channel_id] = message.timestamp
                self._update_rate_tracking(message.server_name)
                self.server_last_activity[message.server_name] = datetime.now()
                
                channel_type = "announcement" if self._is_announcement_channel(message.channel_name) else "regular"
                self.logger.info("📨 NEW WebSocket message queued", 
                               server=message.server_name,
                               channel=message.channel_name,
                               channel_type=channel_type,
                               author=message.author,
                               queue_size=self.message_queue.qsize(),
                               message_hash=message_hash[:8])
                
            except asyncio.TimeoutError:
                self.logger.error("❌ Message queue timeout - dropping WebSocket message",
                                server=message.server_name,
                                queue_size=self.message_queue.qsize())
                self.stats.errors_last_hour += 1
                
        except Exception as e:
            self.logger.error("❌ Error handling WebSocket message",
                            server=message.server_name,
                            error=str(e))
            self.stats.errors_last_hour += 1
    
    def _create_message_hash(self, message: DiscordMessage) -> str:
        """Create unique hash for message"""
        hash_input = (
            f"{message.guild_id}:"
            f"{message.channel_id}:"
            f"{message.message_id}:"
            f"{message.timestamp.isoformat()}:"
            f"{message.author}:"
            f"{hashlib.md5(message.content.encode()).hexdigest()[:8]}"
        )
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    def _is_announcement_channel(self, channel_name: str) -> bool:
        """Проверка что канал является announcement"""
        channel_lower = channel_name.lower()
        return any(keyword in channel_lower for keyword in self.settings.channel_keywords)
    
    def _check_rate_limit(self, server_name: str, is_realtime: bool = False) -> bool:
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
        
        # More relaxed limits for WebSocket real-time messages
        limit = 60 if is_realtime else 30
        
        return len(self.message_rate_tracker[server_name]) < limit
    
    def _update_rate_tracking(self, server_name: str) -> None:
        """Update rate tracking for server"""
        if server_name not in self.message_rate_tracker:
            self.message_rate_tracker[server_name] = []
        
        self.message_rate_tracker[server_name].append(datetime.now())
        
        if server_name not in self.server_message_counts:
            self.server_message_counts[server_name] = 0
        self.server_message_counts[server_name] += 1
    
    async def start(self) -> None:
        """Start the message processor and WebSocket monitoring"""
        if self.running:
            self.logger.warning("⚠️ Message processor is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        self.logger.info("🚀 Starting Message Processor with WebSocket-only system")
        
        # Start background tasks
        self.tasks = [
            asyncio.create_task(self._websocket_message_processor_loop(), name="websocket_processor"),
            asyncio.create_task(self._cleanup_loop(), name="cleanup"),
            asyncio.create_task(self._stats_update_loop(), name="stats"),
            asyncio.create_task(self._health_check_loop(), name="health"),
            asyncio.create_task(self._rate_limit_cleanup_loop(), name="rate_limit_cleanup"),
            asyncio.create_task(self._deduplication_cleanup_loop(), name="dedup_cleanup"),
            asyncio.create_task(self._websocket_stats_loop(), name="websocket_stats")
        ]
        
        # Start Discord WebSocket monitoring
        discord_task = asyncio.create_task(
            self.discord_service.start_websocket_monitoring(), 
            name="discord_websocket"
        )
        self.tasks.append(discord_task)
        
        # Start Telegram bot
        telegram_task = asyncio.create_task(
            self.telegram_service.start_bot_async(), 
            name="telegram_bot"
        )
        self.tasks.append(telegram_task)
        
        # Perform initial sync ТОЛЬКО ОДИН РАЗ (2 сообщения)
        await self._perform_initial_sync_once()
        
        self.logger.info("✅ Message Processor started successfully - WebSocket monitoring active")
        
        try:
            # Wait for all tasks with error isolation
            await asyncio.gather(*self.tasks, return_exceptions=True)
            
            # Log any individual task failures
            for task in self.tasks:
                if task.done() and task.exception():
                    self.logger.error("❌ Task failed",
                                   task_name=task.get_name(),
                                   error=str(task.exception()))
        except Exception as e:
            self.logger.error("❌ Critical error in message processor", error=str(e))
        finally:
            await self.stop()
    
    async def stop(self) -> None:
        """Stop the message processor and clean up"""
        if not self.running:
            return
        
        self.running = False
        self.logger.info("🛑 Stopping Message Processor")
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Cleanup services
        await self.discord_service.cleanup()
        await self.telegram_service.cleanup()
        
        self.logger.info("✅ Message Processor stopped")
    
    async def _perform_initial_sync_once(self) -> None:
        """Выполнить начальную синхронизацию ТОЛЬКО 2 сообщения"""
        if self.initial_sync_completed:
            self.logger.warning("⚠️ Initial sync already completed, skipping")
            return
            
        self.logger.info("🚀 Starting INITIAL synchronization (last 2 messages per channel, ONCE)")
        
        total_messages = 0
        
        for server_name, server_info in self.discord_service.servers.items():
            if server_info.status != ServerStatus.ACTIVE:
                continue
            
            server_messages = []
            
            # Get messages from ALL monitored channels
            for channel_id, channel_info in server_info.accessible_channels.items():
                if channel_id not in self.discord_service.monitored_announcement_channels:
                    continue
                
                try:
                    self.logger.info(f"📥 Getting initial messages from channel {channel_info.channel_name} ({channel_id})")
                    
                    # Get last 2 messages for initial sync
                    messages = await self.discord_service.get_recent_messages(
                        server_name,
                        channel_id,
                        limit=2
                    )
                    
                    if messages:
                        # Сортируем по времени (старые -> новые)
                        messages.sort(key=lambda x: x.timestamp, reverse=False)
                        
                        # Добавляем ВСЕ сообщения в initial sync (без дедупликации)
                        for msg in messages:
                            msg_hash = self._create_message_hash(msg)
                            # Добавляем хэш в processed, чтобы не обрабатывать эти сообщения повторно
                            self.processed_message_hashes.add(msg_hash)
                            server_messages.append(msg)
                        
                        # Запоминаем время последнего сообщения для этого канала
                        latest_message = max(messages, key=lambda x: x.timestamp)
                        self.last_processed_message_per_channel[channel_id] = latest_message.timestamp
                        
                        self.logger.info(f"✅ Channel {channel_info.channel_name}: {len(messages)} messages, latest: {latest_message.timestamp.isoformat()}")
                    else:
                        # Даже если сообщений нет, отмечаем канал как инициализированный
                        self.last_processed_message_per_channel[channel_id] = datetime.now()
                        self.logger.info(f"📭 Channel {channel_info.channel_name}: no messages found")
                    
                    # Отмечаем канал как инициализированный
                    self.channel_initialization_done.add(channel_id)
                    
                except Exception as e:
                    self.logger.error("❌ Error getting initial messages",
                                    server=server_name,
                                    channel_id=channel_id,
                                    error=str(e))
                    # Даже при ошибке отмечаем канал как инициализированный с текущим временем
                    self.last_processed_message_per_channel[channel_id] = datetime.now()
                    self.channel_initialization_done.add(channel_id)
            
            if server_messages:
                # Sort by timestamp and send to Telegram (oldest first)
                server_messages.sort(key=lambda x: x.timestamp, reverse=False)
                sent_count = await self.telegram_service.send_messages_batch(server_messages)
                
                total_messages += sent_count
                self.server_message_counts[server_name] = sent_count
                
                self.logger.info("✅ Initial sync for server complete",
                               server=server_name,
                               messages_sent=sent_count,
                               total_messages=len(server_messages),
                               oldest_first=True)
        
        # ВАЖНО: Отмечаем что начальная синхронизация завершена
        self.initial_sync_completed = True
        
        self.stats.messages_processed_total += total_messages
        
        self.logger.info("🎉 INITIAL synchronization COMPLETED - now monitoring only NEW WebSocket messages",
                        total_messages=total_messages,
                        servers_synced=len([s for s in self.discord_service.servers.values() 
                                          if s.status == ServerStatus.ACTIVE]),
                        initialized_channels=len(self.channel_initialization_done))
    
    async def _websocket_message_processor_loop(self) -> None:
        """WebSocket message processing loop - только НОВЫЕ сообщения"""
        self.logger.info("👂 Starting WebSocket message processor loop")
        
        while self.running:
            try:
                # Get message from queue with timeout
                try:
                    message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Process message immediately for real-time sync
                await self._process_websocket_message(message)
                
                # Mark task as done
                self.message_queue.task_done()
                
            except Exception as e:
                self.logger.error("❌ Error in WebSocket message processor loop", error=str(e))
                await asyncio.sleep(1)
    
    async def _process_websocket_message(self, message: DiscordMessage) -> None:
        """Process a WebSocket Discord message - только НОВЫЕ"""
        try:
            # Проверяем что канал мониторится
            if message.channel_id not in self.discord_service.monitored_announcement_channels:
                self.logger.debug("🚫 Non-monitored channel message skipped in processing",
                                server=message.server_name,
                                channel=message.channel_name)
                return
            
            # Send to Telegram with retry logic
            try:
                success = await self.telegram_service.send_message(message)
                
                if success:
                    self.stats.messages_processed_today += 1
                    self.stats.messages_processed_total += 1
                    self.websocket_messages_processed += 1
                    self.server_message_counts[message.server_name] += 1
                    
                    # Update last processed time for this channel
                    self.last_processed_message_per_channel[message.channel_id] = message.timestamp
                    
                    # Cache in Redis if available
                    if self.redis_client:
                        await self._cache_message_in_redis(message)
                    
                    channel_type = "announcement" if self._is_announcement_channel(message.channel_name) else "regular"
                    self.logger.info("✅ NEW WebSocket message processed and sent",
                                   server=message.server_name,
                                   channel=message.channel_name,
                                   channel_type=channel_type,
                                   author=message.author,
                                   total_today=self.stats.messages_processed_today,
                                   server_total=self.server_message_counts[message.server_name])
                else:
                    self.stats.errors_last_hour += 1
                    self.stats.last_error = "Failed to send WebSocket message to Telegram"
                    self.stats.last_error_time = datetime.now()
                    
                    self.logger.error("❌ Failed to process WebSocket message",
                                    server=message.server_name,
                                    channel=message.channel_name)
            
            except Exception as e:
                self.stats.errors_last_hour += 1
                self.stats.last_error = f"Telegram send error: {str(e)}"
                self.stats.last_error_time = datetime.now()
                
                if "message thread not found" in str(e).lower():
                    self.logger.warning("⚠️ Telegram topic not found for WebSocket message",
                                      server=message.server_name,
                                      channel=message.channel_name)
                else:
                    self.logger.error("❌ Error sending WebSocket message to Telegram",
                                    server=message.server_name,
                                    error=str(e))
            
        except Exception as e:
            self.logger.error("❌ Error processing WebSocket message", 
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
                3600,  # 1 hour TTL
                message.model_dump_json()
            )
        except Exception as e:
            self.logger.error("❌ Failed to cache message in Redis", error=str(e))
    
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
                
                self.logger.info("🧹 Cleanup completed",
                               processed_hashes=len(self.processed_message_hashes),
                               initialized_channels=len(self.channel_initialization_done),
                               websocket_messages_received=self.websocket_messages_received,
                               websocket_messages_processed=self.websocket_messages_processed)
                
            except Exception as e:
                self.logger.error("❌ Error in cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _rate_limit_cleanup_loop(self) -> None:
        """Clean up old rate tracking data"""
        while self.running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                cutoff_time = datetime.now() - timedelta(minutes=5)
                
                for server_name in list(self.message_rate_tracker.keys()):
                    old_count = len(self.message_rate_tracker[server_name])
                    self.message_rate_tracker[server_name] = [
                        ts for ts in self.message_rate_tracker[server_name]
                        if ts > cutoff_time
                    ]
                    
                    new_count = len(self.message_rate_tracker[server_name])
                    
                    if old_count != new_count:
                        self.logger.debug("🧹 Cleaned rate tracking data",
                                        server=server_name,
                                        removed_count=old_count - new_count)
                
            except Exception as e:
                self.logger.error("❌ Error in rate limit cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _deduplication_cleanup_loop(self) -> None:
        """Periodic cleanup of deduplication data"""
        while self.running:
            try:
                await asyncio.sleep(600)  # Every 10 minutes
                
                # Limit the size of processed hashes (keep last 10000)
                async with self.message_dedup_lock:
                    if len(self.processed_message_hashes) > 10000:
                        # Convert to list, sort, and keep last 5000
                        hashes_list = list(self.processed_message_hashes)
                        # Keep only newer half (this is rough, but prevents unlimited growth)
                        self.processed_message_hashes = set(hashes_list[-5000:])
                        
                        self.logger.info("🧹 Cleaned old message hashes",
                                       removed_count=len(hashes_list) - 5000,
                                       remaining_count=len(self.processed_message_hashes))
                
                self.logger.debug("🧹 Deduplication cleanup completed",
                                processed_hashes=len(self.processed_message_hashes),
                                initialized_channels=len(self.channel_initialization_done))
                
            except Exception as e:
                self.logger.error("❌ Error in deduplication cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _stats_update_loop(self) -> None:
        """Statistics update loop"""
        while self.running:
            try:
                await asyncio.sleep(60)  # Update stats every minute
                await self._update_stats()
                
            except Exception as e:
                self.logger.error("❌ Error in stats update loop", error=str(e))
                await asyncio.sleep(60)
    
    async def _websocket_stats_loop(self) -> None:
        """WebSocket statistics update loop"""
        while self.running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                # Get WebSocket status
                ws_status = self.discord_service.get_websocket_status()
                
                self.logger.info("📊 WebSocket Stats Update",
                               active_connections=ws_status.get('active_connections', 0),
                               total_connections=ws_status.get('total_connections', 0),
                               messages_received=self.websocket_messages_received,
                               messages_processed=self.websocket_messages_processed,
                               last_message=self.websocket_last_message_time.isoformat() if self.websocket_last_message_time else None)
                
            except Exception as e:
                self.logger.error("❌ Error in WebSocket stats loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _health_check_loop(self) -> None:
        """Health check loop"""
        while self.running:
            try:
                await asyncio.sleep(self.settings.health_check_interval)
                
                # Check Discord WebSocket health
                ws_status = self.discord_service.get_websocket_status()
                discord_healthy = ws_status.get('active_connections', 0) > 0
                
                # Check Telegram service health  
                telegram_healthy = self.telegram_service.bot_running
                
                # Check queue sizes
                queue_healthy = self.message_queue.qsize() < 500
                
                # Check deduplication health
                dedup_healthy = len(self.processed_message_hashes) < 50000
                
                # Check initialization status
                init_healthy = self.initial_sync_completed
                
                # Check WebSocket message flow
                message_flow_healthy = True
                if self.websocket_last_message_time:
                    time_since_last = datetime.now() - self.websocket_last_message_time
                    # Alert if no messages for 1 hour (this might be normal)
                    message_flow_healthy = time_since_last.total_seconds() < 3600
                
                if not (discord_healthy and telegram_healthy and queue_healthy and dedup_healthy and init_healthy):
                    self.logger.warning("⚠️ Health check failed",
                                      discord_websocket_healthy=discord_healthy,
                                      telegram_healthy=telegram_healthy,
                                      queue_healthy=queue_healthy,
                                      dedup_healthy=dedup_healthy,
                                      init_completed=init_healthy,
                                      message_flow_healthy=message_flow_healthy,
                                      queue_size=self.message_queue.qsize(),
                                      processed_hashes=len(self.processed_message_hashes),
                                      websocket_connections=ws_status.get('active_connections', 0))
                else:
                    self.logger.debug("✅ Health check passed",
                                    queue_size=self.message_queue.qsize(),
                                    processed_hashes=len(self.processed_message_hashes),
                                    initialized_channels=len(self.channel_initialization_done),
                                    websocket_connections=ws_status.get('active_connections', 0))
                
            except Exception as e:
                self.logger.error("❌ Error in health check loop", error=str(e))
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
            self.logger.error("❌ Error updating stats", error=str(e))
    
    def get_status(self) -> Dict[str, any]:
        """Get comprehensive system status with WebSocket info"""
        discord_stats = self.discord_service.get_server_stats()
        
        # Enhanced статистики Telegram
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
        
        # WebSocket статистики
        ws_status = self.discord_service.get_websocket_status()
        
        # Подсчет типов каналов
        announcement_channels = 0
        manually_added_channels = 0
        for server_info in self.discord_service.servers.values():
            for channel_id, channel_info in server_info.accessible_channels.items():
                if channel_id in self.discord_service.monitored_announcement_channels:
                    if self._is_announcement_channel(channel_info.channel_name):
                        announcement_channels += 1
                    else:
                        manually_added_channels += 1
        
        return {
            "system": {
                "running": self.running,
                "uptime_seconds": self.stats.uptime_seconds,
                "memory_usage_mb": self.stats.memory_usage_mb,
                "health_score": self.stats.health_score,
                "status": self.stats.status,
                "realtime_enabled": self.realtime_enabled,
                "initial_sync_completed": self.initial_sync_completed,
                "version": "WebSocket-only версия - real-time messages",
                "mode": "WebSocket Real-time"
            },
            "discord": {
                **discord_stats,
                "auto_discovered_announcement": announcement_channels,
                "manually_added_channels": manually_added_channels,
                "monitoring_strategy": "WebSocket Real-time"
            },
            "telegram": {
                "topics": len(self.telegram_service.server_topics),
                "bot_running": self.telegram_service.bot_running,
                "messages_tracked": len(self.telegram_service.message_mappings),
                "one_topic_per_server": True,
                "enhanced_interface": True
            },
            "processing": {
                "queue_size": self.message_queue.qsize(),
                "messages_today": self.stats.messages_processed_today,
                "messages_total": self.stats.messages_processed_total,
                "errors_last_hour": self.stats.errors_last_hour,
                "last_error": self.stats.last_error,
                "last_error_time": self.stats.last_error_time.isoformat() if self.stats.last_error_time else None,
                "processed_hashes": len(self.processed_message_hashes),
                "initialized_channels": len(self.channel_initialization_done),
                "anti_duplication": "ACTIVE"
            },
            "websocket": {
                "status": ws_status,
                "messages_received": self.websocket_messages_received,
                "messages_processed": self.websocket_messages_processed,
                "last_message_time": self.websocket_last_message_time.isoformat() if self.websocket_last_message_time else None,
                "processing_rate": round(self.websocket_messages_processed / max(1, self.websocket_messages_received) * 100, 2) if self.websocket_messages_received > 0 else 0
            },
            "rate_limiting": {
                "discord": self.discord_service.rate_limiter.get_stats(),
                "telegram": self.telegram_service.rate_limiter.get_stats()
            },
            "enhanced_features": enhanced_features,
            "servers": {
                server_name: {
                    "message_count": self.server_message_counts.get(server_name, 0),
                    "last_activity": self.server_last_activity.get(server_name, datetime.now()).isoformat(),
                    "announcement_channels": len([
                        ch for ch in self.discord_service.servers[server_name].accessible_channels.values()
                        if ch.channel_id in self.discord_service.monitored_announcement_channels
                        and self._is_announcement_channel(ch.channel_name)
                    ]) if server_name in self.discord_service.servers else 0,
                    "manually_added_channels": len([
                        ch for ch in self.discord_service.servers[server_name].accessible_channels.values()
                        if ch.channel_id in self.discord_service.monitored_announcement_channels
                        and not self._is_announcement_channel(ch.channel_name)
                    ]) if server_name in self.discord_service.servers else 0,
                    "initialized_channels": len([
                        ch_id for ch_id in self.discord_service.servers[server_name].accessible_channels.keys()
                        if ch_id in self.channel_initialization_done
                    ]) if server_name in self.discord_service.servers else 0
                }
                for server_name in self.discord_service.servers.keys()
            },
            "anti_duplication_info": {
                "initial_sync_completed": self.initial_sync_completed,
                "initialized_channels": len(self.channel_initialization_done),
                "processed_message_hashes": len(self.processed_message_hashes),
                "channel_timestamps": {
                    ch_id: timestamp.isoformat() 
                    for ch_id, timestamp in self.last_processed_message_per_channel.items()
                },
                "strategy": "WebSocket Real-time - no polling, no duplicates"
            }
        }
    
    # Остальные методы остаются без изменений
    def reset_channel_initialization(self, channel_id: str) -> bool:
        """Сброс инициализации канала (для debug/maintenance)"""
        try:
            if channel_id in self.channel_initialization_done:
                self.channel_initialization_done.remove(channel_id)
            
            if channel_id in self.last_processed_message_per_channel:
                del self.last_processed_message_per_channel[channel_id]
            
            self.logger.info("Channel initialization reset", channel_id=channel_id)
            return True
            
        except Exception as e:
            self.logger.error("Error resetting channel initialization", 
                            channel_id=channel_id, error=str(e))
            return False
    
    def force_reinitialize_all_channels(self) -> int:
        """Принудительная реинициализация всех каналов"""
        try:
            old_count = len(self.channel_initialization_done)
            
            self.channel_initialization_done.clear()
            self.last_processed_message_per_channel.clear()
            self.processed_message_hashes.clear()
            self.initial_sync_completed = False
            
            # Reset WebSocket stats
            self.websocket_messages_received = 0
            self.websocket_messages_processed = 0
            self.websocket_last_message_time = None
            
            self.logger.warning("🔄 Forced reinitialization of all channels (WebSocket mode)", 
                              previous_initialized_count=old_count)
            
            return old_count
            
        except Exception as e:
            self.logger.error("❌ Error in force reinitialize", error=str(e))
            return 0
    
    def get_channel_status(self, channel_id: str) -> Dict[str, any]:
        """Получить статус конкретного канала"""
        return {
            "channel_id": channel_id,
            "is_monitored": channel_id in self.discord_service.monitored_announcement_channels,
            "is_initialized": channel_id in self.channel_initialization_done,
            "last_processed_timestamp": self.last_processed_message_per_channel.get(channel_id),
            "last_processed_iso": self.last_processed_message_per_channel.get(channel_id).isoformat() 
                                 if channel_id in self.last_processed_message_per_channel else None,
            "in_server": any(
                channel_id in server_info.channels.keys() 
                for server_info in self.discord_service.servers.values()
            ),
            "websocket_coverage": True  # All monitored channels have WebSocket coverage
        }
    
    def get_anti_duplication_stats(self) -> Dict[str, any]:
        """Подробная статистика системы анти-дублирования"""
        return {
            "initialization": {
                "initial_sync_completed": self.initial_sync_completed,
                "initialized_channels_count": len(self.channel_initialization_done),
                "initialized_channels": list(self.channel_initialization_done),
                "pending_channels": [
                    ch_id for ch_id in self.discord_service.monitored_announcement_channels
                    if ch_id not in self.channel_initialization_done
                ]
            },
            "deduplication": {
                "processed_hashes_count": len(self.processed_message_hashes),
                "tracked_channels_count": len(self.last_processed_message_per_channel),
                "memory_usage_estimate_mb": len(self.processed_message_hashes) * 64 / (1024 * 1024)
            },
            "timestamps": {
                "channels_with_timestamps": len(self.last_processed_message_per_channel),
                "oldest_timestamp": min(self.last_processed_message_per_channel.values()).isoformat() 
                                  if self.last_processed_message_per_channel else None,
                "newest_timestamp": max(self.last_processed_message_per_channel.values()).isoformat() 
                                  if self.last_processed_message_per_channel else None
            },
            "websocket": {
                "messages_received": self.websocket_messages_received,
                "messages_processed": self.websocket_messages_processed,
                "last_message_time": self.websocket_last_message_time.isoformat() if self.websocket_last_message_time else None,
                "processing_efficiency": round(self.websocket_messages_processed / max(1, self.websocket_messages_received) * 100, 2) if self.websocket_messages_received > 0 else 100
            },
            "system_health": {
                "queue_size": self.message_queue.qsize(),
                "rate_tracking_active": len(self.message_rate_tracker),
                "system_running": self.running,
                "realtime_enabled": self.realtime_enabled,
                "websocket_status": self.discord_service.get_websocket_status()
            }
        }
    
    def get_websocket_performance_stats(self) -> Dict[str, any]:
        """Получить статистику производительности WebSocket"""
        ws_status = self.discord_service.get_websocket_status()
        
        # Calculate message processing rate
        processing_rate = 0
        if self.websocket_messages_received > 0:
            processing_rate = round(self.websocket_messages_processed / self.websocket_messages_received * 100, 2)
        
        # Calculate uptime for each connection
        connection_uptimes = []
        for conn in ws_status.get('connections', []):
            if conn.get('connected') and conn.get('connected_at'):
                try:
                    connected_at = datetime.fromisoformat(conn['connected_at']) if isinstance(conn['connected_at'], str) else conn['connected_at']
                    uptime_seconds = (datetime.now() - connected_at).total_seconds()
                    connection_uptimes.append(uptime_seconds)
                except:
                    pass
        
        return {
            "websocket_connections": {
                "total": ws_status.get('total_connections', 0),
                "active": ws_status.get('active_connections', 0),
                "success_rate": round(ws_status.get('active_connections', 0) / max(1, ws_status.get('total_tokens', 1)) * 100, 2)
            },
            "message_processing": {
                "received": self.websocket_messages_received,
                "processed": self.websocket_messages_processed,
                "processing_rate_percent": processing_rate,
                "queue_size": self.message_queue.qsize(),
                "last_message": self.websocket_last_message_time.isoformat() if self.websocket_last_message_time else None
            },
            "connection_health": {
                "average_uptime_seconds": sum(connection_uptimes) / len(connection_uptimes) if connection_uptimes else 0,
                "total_connections_attempted": len(self.discord_service.gateway_urls),
                "successful_connections": ws_status.get('active_connections', 0),
                "monitored_channels": len(self.discord_service.monitored_announcement_channels)
            },
            "performance_indicators": {
                "real_time_delivery": processing_rate >= 95,
                "connection_stability": ws_status.get('active_connections', 0) >= len(self.discord_service.gateway_urls) * 0.8,
                "queue_health": self.message_queue.qsize() < 100,
                "deduplication_efficiency": len(self.processed_message_hashes) < 10000
            }
        }