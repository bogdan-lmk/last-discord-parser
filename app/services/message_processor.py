# app/services/message_processor.py - ИСПРАВЛЕННАЯ ВЕРСИЯ без дублирования
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
    """Главный оркестратор - БЕЗ дублирования сообщений"""
    
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
        
        # ИСПРАВЛЕНИЕ: Улучшенная дедупликация с временными метками
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.batch_queue: List[DiscordMessage] = []
        self.redis_client = redis_client
        
        # НОВОЕ: Отслеживание последних обработанных сообщений по каналам
        self.last_processed_message_per_channel: Dict[str, datetime] = {}  # channel_id -> last_timestamp
        self.channel_initialization_done: Set[str] = set()  # каналы, для которых выполнена начальная инициализация
        
        # ИСПРАВЛЕНИЕ: Глобальная дедупликация с уникальными хэшами
        self.processed_message_hashes: Set[str] = set()  # Только хэши уже обработанных сообщений
        self.message_dedup_lock = asyncio.Lock()
        
        # Real-time synchronization state
        self.realtime_enabled = True
        self.last_sync_times: Dict[str, datetime] = {}  # server_name -> last_sync
        self.sync_intervals: Dict[str, int] = {}  # server_name -> interval_seconds
        
        # НОВОЕ: Более точная статистика по серверам
        self.server_message_counts: Dict[str, int] = {}  # server_name -> count
        self.server_last_activity: Dict[str, datetime] = {}  # server_name -> timestamp
        
        # Message rate tracking
        self.message_rate_tracker: Dict[str, List[datetime]] = {}  # server -> timestamps
        
        # Periodic cleanup
        self.last_cleanup = datetime.now()
        
        # НОВОЕ: Флаг завершения инициализации
        self.initial_sync_completed = False
        
    async def initialize(self) -> bool:
        """Initialize all services and set up real-time integration"""
        self.logger.info("Initializing Message Processor with anti-duplication system")
        
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
        
        self.logger.info("Message Processor initialized successfully",
                        discord_servers=len(self.discord_service.servers),
                        telegram_topics=len(self.telegram_service.server_topics),
                        realtime_enabled=self.realtime_enabled,
                        monitored_channels=len(self.discord_service.monitored_announcement_channels),
                        anti_duplication="ACTIVE")
        
        return True
    
    async def _handle_realtime_message(self, message: DiscordMessage) -> None:
        """ИСПРАВЛЕНО: Handle real-time message - только НОВЫЕ сообщения"""
        try:
            # ПРОВЕРКА 1: Канал должен быть мониторимым
            if message.channel_id not in self.discord_service.monitored_announcement_channels:
                self.logger.debug("Message from non-monitored channel ignored",
                                server=message.server_name,
                                channel=message.channel_name,
                                channel_id=message.channel_id)
                return
            
            # ПРОВЕРКА 2: Инициализация канала должна быть завершена
            if not self.initial_sync_completed:
                self.logger.debug("Initial sync not completed yet, queuing real-time message",
                                server=message.server_name,
                                channel=message.channel_name)
                # Пока инициализация не завершена, ставим в очередь
                try:
                    await asyncio.wait_for(self.message_queue.put(message), timeout=1.0)
                except asyncio.TimeoutError:
                    self.logger.warning("Real-time queue full during initialization")
                return
            
            # ПРОВЕРКА 3: Канал должен быть инициализирован
            if message.channel_id not in self.channel_initialization_done:
                self.logger.debug("Channel not initialized yet, skipping real-time message",
                                channel_id=message.channel_id)
                return
            
            # ПРОВЕРКА 4: Сообщение должно быть новее последнего обработанного
            last_processed = self.last_processed_message_per_channel.get(message.channel_id)
            if last_processed and message.timestamp <= last_processed:
                self.logger.debug("Real-time message is not newer than last processed, skipping",
                                channel_id=message.channel_id,
                                message_timestamp=message.timestamp.isoformat(),
                                last_processed_timestamp=last_processed.isoformat())
                return
            
            # ПРОВЕРКА 5: Дедупликация по хэшу
            message_hash = self._create_message_hash(message)
            async with self.message_dedup_lock:
                if message_hash in self.processed_message_hashes:
                    self.logger.debug("Duplicate real-time message ignored",
                                    message_hash=message_hash[:8],
                                    server=message.server_name)
                    return
                
                # Добавляем в processed хэши
                self.processed_message_hashes.add(message_hash)
            
            # ПРОВЕРКА 6: Rate limiting
            if not self._check_rate_limit(message.server_name, is_realtime=True):
                self.logger.warning("Rate limit exceeded for real-time message", 
                                  server=message.server_name)
                return
            
            # ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ - обрабатываем сообщение
            try:
                await asyncio.wait_for(
                    self.message_queue.put(message), 
                    timeout=2.0
                )
                
                # Обновляем последнее обработанное время для канала
                self.last_processed_message_per_channel[message.channel_id] = message.timestamp
                
                # Update tracking
                self._update_rate_tracking(message.server_name)
                self.server_last_activity[message.server_name] = datetime.now()
                
                channel_type = "announcement" if self._is_announcement_channel(message.channel_name) else "regular"
                self.logger.info("NEW real-time message queued", 
                               server=message.server_name,
                               channel=message.channel_name,
                               channel_type=channel_type,
                               author=message.author,
                               queue_size=self.message_queue.qsize(),
                               message_hash=message_hash[:8])
                
            except asyncio.TimeoutError:
                self.logger.error("Message queue timeout - dropping real-time message",
                                server=message.server_name,
                                queue_size=self.message_queue.qsize())
                self.stats.errors_last_hour += 1
                
        except Exception as e:
            self.logger.error("Error handling real-time message",
                            server=message.server_name,
                            error=str(e))
            self.stats.errors_last_hour += 1
    
    def _create_message_hash(self, message: DiscordMessage) -> str:
        """ИСПРАВЛЕНО: Create unique hash for message"""
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
        return channel_lower in self.settings.channel_keywords
    
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
        
        # More relaxed limits for real-time messages
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
        """Start the message processor and all background tasks"""
        if self.running:
            self.logger.warning("Message processor is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        self.logger.info("Starting Message Processor with anti-duplication system")
        
        # Start background tasks
        self.tasks = [
            asyncio.create_task(self._realtime_message_processor_loop()),
            asyncio.create_task(self._batch_processor_loop()),
            asyncio.create_task(self._periodic_sync_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._stats_update_loop()),
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._rate_limit_cleanup_loop()),
            asyncio.create_task(self._deduplication_cleanup_loop())
        ]
        
        # Start Discord monitoring (HTTP polling for monitored channels)
        discord_task = asyncio.create_task(self.discord_service.start_websocket_monitoring())
        self.tasks.append(discord_task)
        
        # Start Telegram bot asynchronously
        telegram_task = asyncio.create_task(self.telegram_service.start_bot_async())
        self.tasks.append(telegram_task)
        
        # ИСПРАВЛЕНИЕ: Perform initial sync ТОЛЬКО ОДИН РАЗ
        await self._perform_initial_sync_once()
        
        self.logger.info("Message Processor started successfully - ready for real-time monitoring")
        
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
    
    async def _perform_initial_sync_once(self) -> None:
        """ИСПРАВЛЕНО: Выполнить начальную синхронизацию ТОЛЬКО ОДИН РАЗ"""
        if self.initial_sync_completed:
            self.logger.warning("Initial sync already completed, skipping")
            return
            
        self.logger.info("🚀 Starting INITIAL synchronization (last 5 messages per channel, ONCE)")
        
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
                    
                    # Get last 5 messages for initial sync
                    messages = await self.discord_service.get_recent_messages(
                        server_name,
                        channel_id,
                        limit=5
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
                    self.logger.error("Error getting initial messages",
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
                self.last_sync_times[server_name] = datetime.now()
                self.server_message_counts[server_name] = sent_count
                
                self.logger.info("Initial sync for server complete",
                               server=server_name,
                               messages_sent=sent_count,
                               total_messages=len(server_messages),
                               oldest_first=True)
        
        # ВАЖНО: Отмечаем что начальная синхронизация завершена
        self.initial_sync_completed = True
        
        self.stats.messages_processed_total += total_messages
        
        self.logger.info("🎉 INITIAL synchronization COMPLETED - now monitoring only NEW messages",
                        total_messages=total_messages,
                        servers_synced=len([s for s in self.discord_service.servers.values() 
                                          if s.status == ServerStatus.ACTIVE]),
                        initialized_channels=len(self.channel_initialization_done))
    
    async def _realtime_message_processor_loop(self) -> None:
        """Real-time message processing loop - только НОВЫЕ сообщения"""
        self.logger.info("Starting real-time message processor loop (NEW messages only)")
        
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
        """ИСПРАВЛЕНО: Process a real-time Discord message - только НОВЫЕ"""
        try:
            # Проверяем что канал мониторится
            if message.channel_id not in self.discord_service.monitored_announcement_channels:
                self.logger.debug("Non-monitored channel message skipped in processing",
                                server=message.server_name,
                                channel=message.channel_name)
                return
            
            # Send to Telegram immediately
            success = await self.telegram_service.send_message(message)
            
            if success:
                self.stats.messages_processed_today += 1
                self.stats.messages_processed_total += 1
                self.server_message_counts[message.server_name] += 1
                
                # Update last processed time for this channel
                self.last_processed_message_per_channel[message.channel_id] = message.timestamp
                
                # Cache in Redis if available
                if self.redis_client:
                    await self._cache_message_in_redis(message)
                
                channel_type = "announcement" if self._is_announcement_channel(message.channel_name) else "regular"
                self.logger.info("✅ NEW real-time message processed and sent",
                               server=message.server_name,
                               channel=message.channel_name,
                               channel_type=channel_type,
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
            message_hash = self._create_message_hash(message)
            await self.redis_client.setex(
                f"msg:{message_hash}",
                3600,  # 1 hour TTL
                message.model_dump_json()
            )
        except Exception as e:
            self.logger.error("Failed to cache message in Redis", error=str(e))
    
    async def _batch_processor_loop(self) -> None:
        """Batch message processing loop - отключен для избежания дублирования"""
        # ИСПРАВЛЕНИЕ: Отключаем batch processing, чтобы избежать дублирование
        # Все сообщения обрабатываются через real-time processor
        while self.running:
            await asyncio.sleep(60)  # Just sleep, don't process anything
    
    async def _periodic_sync_loop(self) -> None:
        """ИСПРАВЛЕНО: Periodic fallback sync - только для missed messages"""
        while self.running:
            try:
                # Run fallback check every 10 minutes (только если real-time не работает)
                await asyncio.sleep(600)
                
                # Только если initial sync завершен
                if not self.initial_sync_completed:
                    continue
                
                self.logger.debug("Checking for missed messages (fallback sync)")
                
                # Check each server for VERY recent missed messages (last 2 minutes)
                cutoff_time = datetime.now() - timedelta(minutes=2)
                
                for server_name, last_sync in self.last_sync_times.items():
                    try:
                        # Skip if recently synced via real-time
                        if (datetime.now() - last_sync).total_seconds() < 120:
                            continue
                        
                        await self._sync_server_missed_messages_only(server_name, cutoff_time)
                        
                    except Exception as e:
                        self.logger.error("Error in periodic sync for server",
                                        server=server_name,
                                        error=str(e))
                
            except Exception as e:
                self.logger.error("Error in periodic sync loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _sync_server_missed_messages_only(self, server_name: str, cutoff_time: datetime) -> None:
        """ИСПРАВЛЕНО: Sync only very recent missed messages"""
        if server_name not in self.discord_service.servers:
            return
        
        server_info = self.discord_service.servers[server_name]
        if server_info.status != ServerStatus.ACTIVE:
            return
        
        missed_messages = []
        
        # Check each monitored channel for very recent messages
        for channel_id, channel_info in server_info.accessible_channels.items():
            if channel_id not in self.discord_service.monitored_announcement_channels:
                continue
            
            try:
                # Get only very recent messages (last 2 minutes)
                messages = await self.discord_service.get_recent_messages(
                    server_name, channel_id, limit=3
                )
                
                last_processed = self.last_processed_message_per_channel.get(channel_id)
                
                for msg in messages:
                    # Only if message is newer than cutoff AND newer than last processed
                    if (msg.timestamp > cutoff_time and 
                        (not last_processed or msg.timestamp > last_processed)):
                        
                        msg_hash = self._create_message_hash(msg)
                        
                        # Check if not already processed
                        async with self.message_dedup_lock:
                            if msg_hash not in self.processed_message_hashes:
                                missed_messages.append(msg)
                                self.processed_message_hashes.add(msg_hash)
                                # Update last processed time
                                self.last_processed_message_per_channel[channel_id] = msg.timestamp
                
            except Exception as e:
                self.logger.error("Error in missed messages check",
                                server=server_name,
                                channel_id=channel_id,
                                error=str(e))
        
        if missed_messages:
            # Sort and send
            missed_messages.sort(key=lambda x: x.timestamp, reverse=False)
            sent_count = await self.telegram_service.send_messages_batch(missed_messages)
            
            self.logger.info("Fallback sync found missed messages",
                       server=server_name,
                       messages_sent=sent_count,
                       total_messages=len(missed_messages))
            
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
                               processed_hashes=len(self.processed_message_hashes),
                               initialized_channels=len(self.channel_initialization_done))
                
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
                        
                        self.logger.info("Cleaned old message hashes",
                                       removed_count=len(hashes_list) - 5000,
                                       remaining_count=len(self.processed_message_hashes))
                
                # Clean old rate tracking data
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
                                processed_hashes=len(self.processed_message_hashes),
                                initialized_channels=len(self.channel_initialization_done))
                
            except Exception as e:
                self.logger.error("Error in deduplication cleanup loop", error=str(e))
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
                dedup_healthy = len(self.processed_message_hashes) < 50000
                
                # Check initialization status
                init_healthy = self.initial_sync_completed
                
                if not (discord_healthy and telegram_healthy and queue_healthy and dedup_healthy and init_healthy):
                    self.logger.warning("Health check failed",
                                      discord_healthy=discord_healthy,
                                      telegram_healthy=telegram_healthy,
                                      queue_healthy=queue_healthy,
                                      dedup_healthy=dedup_healthy,
                                      init_completed=init_healthy,
                                      queue_size=self.message_queue.qsize(),
                                      processed_hashes=len(self.processed_message_hashes))
                else:
                    self.logger.debug("Health check passed",
                                    queue_size=self.message_queue.qsize(),
                                    processed_hashes=len(self.processed_message_hashes),
                                    initialized_channels=len(self.channel_initialization_done))
                
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
        """Get comprehensive system status with anti-duplication info"""
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
                "version": "Anti-duplication версия - только новые сообщения"
            },
            "discord": {
                **discord_stats,
                "auto_discovered_announcement": announcement_channels,
                "manually_added_channels": manually_added_channels,
                "monitoring_strategy": "auto announcement + manual any"
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
                "batch_size": len(self.batch_queue),
                "messages_today": self.stats.messages_processed_today,
                "messages_total": self.stats.messages_processed_total,
                "errors_last_hour": self.stats.errors_last_hour,
                "last_error": self.stats.last_error,
                "last_error_time": self.stats.last_error_time.isoformat() if self.stats.last_error_time else None,
                "processed_hashes": len(self.processed_message_hashes),
                "initialized_channels": len(self.channel_initialization_done),
                "anti_duplication": "ACTIVE"
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
                    "last_sync": self.last_sync_times.get(server_name, datetime.now()).isoformat(),
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
                "strategy": "Initial sync (last 5) + real-time only NEW messages"
            }
        }
    
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
            
            self.logger.warning("Forced reinitialization of all channels", 
                              previous_initialized_count=old_count)
            
            return old_count
            
        except Exception as e:
            self.logger.error("Error in force reinitialize", error=str(e))
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
            )
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
            "system_health": {
                "queue_size": self.message_queue.qsize(),
                "rate_tracking_active": len(self.message_rate_tracker),
                "system_running": self.running,
                "realtime_enabled": self.realtime_enabled
            }
        }