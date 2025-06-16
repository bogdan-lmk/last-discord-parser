# app/services/telegram_service.py - ИСПРАВЛЕНИЕ ИМПОРТОВ
import asyncio
import json
from datetime import datetime, timedelta  # ИСПРАВЛЕНО: добавлен timedelta
from typing import Dict, List, Optional, Callable
from threading import Lock
import structlog
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from ..models.message import DiscordMessage
from ..models.server import ServerInfo
from ..config import Settings
from ..utils.rate_limiter import RateLimiter

class TelegramService:
    """ИСПРАВЛЕННЫЙ Telegram service с правильной логикой топиков"""
    
    def __init__(self, 
                 settings: Settings,
                 rate_limiter: RateLimiter,
                 redis_client = None,
                 logger = None):
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.redis_client = redis_client
        self.logger = logger or structlog.get_logger(__name__)
        
        # Telegram Bot
        self.bot = telebot.TeleBot(
            self.settings.telegram_bot_token,
            skip_pending=True,
            threaded=True
        )
        
        # ИСПРАВЛЕНИЕ: Правильное управление топиками
        self.server_topics: Dict[str, int] = {}  # server_name -> topic_id (ОДИН топик на сервер)
        self._async_lock = asyncio.Lock()
        self.user_states: Dict[int, dict] = {}  # user_id -> state
        
        # Message tracking
        self.message_mappings: Dict[str, int] = {}  # timestamp -> telegram_message_id
        
        # Callbacks
        self.new_message_callbacks: List[Callable[[DiscordMessage], None]] = []
        
        # Bot running state
        self.bot_running = False
        
        # ИСПРАВЛЕНИЕ: Кэш проверки топиков с блокировкой дублирования
        self._topic_verification_cache: Dict[int, bool] = {}
        self._topic_creation_lock: Dict[str, asyncio.Lock] = {}  # server_name -> lock
        self._topic_cache_timeout = 300  # 5 минут
        self._last_cache_clear = datetime.now()
        
        # ИСПРАВЛЕНИЕ: Дедупликация сообщений
        self._processed_messages: Dict[str, datetime] = {}  # message_id -> timestamp
        self._message_dedup_lock = asyncio.Lock()
    
    async def initialize(self) -> bool:
        """Initialize Telegram service"""
        try:
            # Test bot token
            bot_info = self.bot.get_me()
            self.logger.info("Telegram bot initialized", 
                           bot_username=bot_info.username,
                           bot_id=bot_info.id)
            
            # Load persistent data
            await self._load_persistent_data()
            
            # Verify chat access and clean invalid topics
            if await self._verify_chat_access():
                self.logger.info("Chat access verified", 
                               chat_id=self.settings.telegram_chat_id)
                
                # ИСПРАВЛЕНИЕ: Очищаем неверные топики при инициализации
                cleaned_count = await self._clean_invalid_topics()
                if cleaned_count > 0:
                    self.logger.info("Cleaned invalid topics on startup", count=cleaned_count)
                
                return True
            else:
                self.logger.error("Cannot access Telegram chat", 
                                chat_id=self.settings.telegram_chat_id)
                return False
                
        except Exception as e:
            self.logger.error("Telegram service initialization failed", error=str(e))
            return False
    
    async def _verify_chat_access(self) -> bool:
        """Verify that bot can access the configured chat"""
        try:
            chat = self.bot.get_chat(self.settings.telegram_chat_id)
            
            # Check if it's a supergroup with topics
            if hasattr(chat, 'is_forum') and chat.is_forum:
                self.logger.info("Chat supports topics", chat_type=chat.type)
                return True
            elif chat.type in ['group', 'supergroup']:
                self.logger.warning("Chat does not support topics", 
                                  chat_type=chat.type,
                                  note="Topics disabled, will use regular messages")
                return True
            else:
                self.logger.error("Invalid chat type", chat_type=chat.type)
                return False
                
        except Exception as e:
            self.logger.error("Chat verification failed", error=str(e))
            return False
    
    async def _load_persistent_data(self) -> None:
        """Load persistent data from Redis or file"""
        try:
            if self.redis_client:
                # Load from Redis
                data = await self._load_from_redis()
            else:
                # Load from file
                data = self._load_from_file()
            
            if data:
                self.server_topics = data.get('topics', {})
                self.message_mappings = data.get('messages', {})
                
                self.logger.info("Loaded persistent data", 
                               topics=len(self.server_topics),
                               messages=len(self.message_mappings))
            
        except Exception as e:
            self.logger.error("Failed to load persistent data", error=str(e))
    
    def _load_from_file(self) -> Optional[dict]:
        """Load data from JSON file"""
        try:
            with open('telegram_data.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as e:
            self.logger.error("Error loading from file", error=str(e))
            return {}
    
    async def _load_from_redis(self) -> Optional[dict]:
        """Load data from Redis"""
        try:
            data = await self.redis_client.get('telegram_data')
            return json.loads(data) if data else {}
        except Exception as e:
            self.logger.error("Error loading from Redis", error=str(e))
            return {}
    
    async def _save_persistent_data(self) -> None:
        """Save persistent data"""
        data = {
            'topics': self.server_topics,
            'messages': self.message_mappings,
            'last_updated': datetime.now().isoformat()
        }
        
        try:
            if self.redis_client:
                await self.redis_client.setex(
                    'telegram_data', 
                    self.settings.cache_ttl_seconds,
                    json.dumps(data)
                )
            else:
                with open('telegram_data.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                    
        except Exception as e:
            self.logger.error("Failed to save persistent data", error=str(e))
    
    async def send_message(self, message: DiscordMessage) -> bool:
        """ИСПРАВЛЕНО: Send a Discord message to Telegram (с дедупликацией)"""
        try:
            # ИСПРАВЛЕНИЕ: Проверяем дубли сообщений
            if await self._is_duplicate_message(message):
                self.logger.debug("Duplicate message ignored", 
                               server=message.server_name,
                               message_id=message.message_id)
                return True  # Считаем успешным, но не отправляем
            
            await self.rate_limiter.wait_if_needed("telegram_send")
            
            # ИСПРАВЛЕНИЕ: Получаем топик ТОЛЬКО ОДИН РАЗ для сервера
            topic_id = await self._get_or_create_server_topic(message.server_name)
            
            if topic_id is None and self.settings.use_topics:
                self.logger.error("Failed to get topic for server", server=message.server_name)
                return False
            
            # Format message
            formatted_message = self._format_message_for_telegram(message)
            
            # ИСПРАВЛЕНИЕ: Убираем Markdown для безопасности
            sent_message = self.bot.send_message(
                chat_id=self.settings.telegram_chat_id,
                text=formatted_message,
                message_thread_id=topic_id if self.settings.use_topics else None,
                parse_mode=None  # ИСПРАВЛЕНО: убираем Markdown
            )
            
            # Track message
            if sent_message:
                self.message_mappings[str(message.timestamp)] = sent_message.message_id
                
                # ИСПРАВЛЕНИЕ: Отмечаем сообщение как обработанное
                await self._mark_message_as_processed(message)
                
                # Сохраняем данные
                asyncio.create_task(self._save_persistent_data())
                
                self.logger.info("Message sent to Telegram", 
                               server=message.server_name,
                               channel=message.channel_name,
                               telegram_message_id=sent_message.message_id,
                               topic_id=topic_id)
                
                self.rate_limiter.record_success()
                return True
            
        except Exception as e:
            self.logger.error("Failed to send message to Telegram", 
                            server=message.server_name,
                            error=str(e))
            self.rate_limiter.record_error()
            return False
        
        return False
    
    async def _is_duplicate_message(self, message: DiscordMessage) -> bool:
        """ИСПРАВЛЕНИЕ: Проверка дублирования сообщений"""
        async with self._message_dedup_lock:
            message_key = f"{message.guild_id}:{message.channel_id}:{message.message_id}"
            
            # Очищаем старые записи (старше 1 часа)
            cutoff_time = datetime.now() - timedelta(hours=1)
            old_keys = [
                key for key, timestamp in self._processed_messages.items()
                if timestamp < cutoff_time
            ]
            for key in old_keys:
                del self._processed_messages[key]
            
            # Проверяем дубли
            if message_key in self._processed_messages:
                return True
            
            return False
    
    async def _mark_message_as_processed(self, message: DiscordMessage) -> None:
        """ИСПРАВЛЕНИЕ: Отмечаем сообщение как обработанное"""
        async with self._message_dedup_lock:
            message_key = f"{message.guild_id}:{message.channel_id}:{message.message_id}"
            self._processed_messages[message_key] = datetime.now()
    
    def _format_message_for_telegram(self, message: DiscordMessage) -> str:
        """ИСПРАВЛЕНО: Format Discord message for Telegram без Markdown"""
        parts = []
        
        # ИСПРАВЛЕНИЕ: Простое форматирование без Markdown
        parts.append(f"📢 #{message.channel_name}")
        
        if self.settings.show_timestamps:
            parts.append(f"📅 {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        parts.append(f"👤 {message.author}")
        parts.append(f"💬 {message.content}")
        
        return "\n".join(parts)
    async def send_messages_batch(self, messages: List[DiscordMessage]) -> int:
        """ИСПРАВЛЕНО: Send multiple messages as a batch"""
        if not messages:
            return 0
        
        # ИСПРАВЛЕНИЕ: Группируем по серверам для правильного распределения по топикам
        server_groups = {}
        for message in messages:
            server_name = message.server_name
            if server_name not in server_groups:
                server_groups[server_name] = []
            server_groups[server_name].append(message)
        
        sent_count = 0
        
        # Send messages grouped by server
        for server_name, server_messages in server_groups.items():
            self.logger.info("Sending message batch", 
                           server=server_name,
                           message_count=len(server_messages))
            
            # Sort messages chronologically
            server_messages.sort(key=lambda x: x.timestamp)
            
            # ИСПРАВЛЕНИЕ: Получаем топик ОДИН раз для всех сообщений сервера
            topic_id = await self._get_or_create_server_topic(server_name)
            
            if topic_id is None and self.settings.use_topics:
                self.logger.error("Failed to get topic for server batch", server=server_name)
                continue
            
            # Send each message to the SAME topic
            for message in server_messages:
                if await self._send_message_to_topic(message, topic_id):
                    sent_count += 1
                
                # Rate limiting between messages
                await asyncio.sleep(0.1)
        
        self.logger.info("Batch sending complete", 
                       total_messages=len(messages),
                       sent_messages=sent_count)
        
        return sent_count
    
    async def _send_message_to_topic(self, message: DiscordMessage, topic_id: Optional[int]) -> bool:
        """Send message to specific topic (helper method)"""
        try:
            # ИСПРАВЛЕНИЕ: Проверяем дубли
            if await self._is_duplicate_message(message):
                self.logger.debug("Duplicate message in batch ignored", 
                               server=message.server_name,
                               message_id=message.message_id)
                return True
            
            await self.rate_limiter.wait_if_needed("telegram_send")
            
            # Format message
            formatted_message = self._format_message_for_telegram(message)
            
            # Send message без Markdown
            sent_message = self.bot.send_message(
                chat_id=self.settings.telegram_chat_id,
                text=formatted_message,
                message_thread_id=topic_id if self.settings.use_topics else None,
                parse_mode=None
            )
            
            # Track message
            if sent_message:
                self.message_mappings[str(message.timestamp)] = sent_message.message_id
                
                # Отмечаем как обработанное
                await self._mark_message_as_processed(message)
                
                self.rate_limiter.record_success()
                return True
            
        except Exception as e:
            self.logger.error("Failed to send message to topic", 
                            server=message.server_name,
                            topic_id=topic_id,
                            error=str(e))
            self.rate_limiter.record_error()
            return False
        
        return False
    
    async def _get_or_create_server_topic(self, server_name: str) -> Optional[int]:
        """КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Получить или создать топик для сервера (БЕЗ дублирования!)"""
        if not self.settings.use_topics:
            return None
        
        # ИСПРАВЛЕНИЕ: Блокировка по серверу для предотвращения дублирования
        if server_name not in self._topic_creation_lock:
            self._topic_creation_lock[server_name] = asyncio.Lock()
        
        async with self._topic_creation_lock[server_name]:
            # Очистка кэша если нужно
            await self._clear_topic_cache_if_needed()
            
            # ИСПРАВЛЕНИЕ: Проверяем существующий топик
            if server_name in self.server_topics:
                topic_id = self.server_topics[server_name]
                
                # Проверяем существование топика (с кэшированием)
                if await self._verify_topic_exists_cached(topic_id):
                    self.logger.debug("Using existing topic", 
                                    server=server_name, 
                                    topic_id=topic_id)
                    return topic_id
                else:
                    # Топик был удален, убираем из кэша
                    self.logger.warning("Topic was deleted, removing from cache", 
                                      server=server_name, 
                                      topic_id=topic_id)
                    del self.server_topics[server_name]
                    if topic_id in self._topic_verification_cache:
                        del self._topic_verification_cache[topic_id]
            
            # ИСПРАВЛЕНИЕ: Проверяем еще раз - может другой поток уже создал
            if server_name in self.server_topics:
                topic_id = self.server_topics[server_name]
                if await self._verify_topic_exists_cached(topic_id):
                    self.logger.debug("Topic created by another thread", 
                                    server=server_name, 
                                    topic_id=topic_id)
                    return topic_id
            
            # Создаем новый топик только если его действительно нет
            try:
                self.logger.info("Creating new topic for server", server=server_name)
                
                # ИСПРАВЛЕНИЕ: Упрощаем создание топика
                topic = self.bot.create_forum_topic(
                    chat_id=self.settings.telegram_chat_id,
                    name=f"🏰 {server_name}",
                    icon_color=0x6FB9F0
                )
                
                topic_id = topic.message_thread_id
                
                # ИСПРАВЛЕНИЕ: Атомарно сохраняем в кэш
                self.server_topics[server_name] = topic_id
                self._topic_verification_cache[topic_id] = True
                
                # Сохраняем в persistent storage асинхронно
                asyncio.create_task(self._save_persistent_data())
                
                self.logger.info("Created new topic successfully", 
                               server=server_name,
                               topic_id=topic_id)
                
                return topic_id
                
            except Exception as e:
                self.logger.error("Failed to create topic", 
                                server=server_name,
                                error=str(e))
                return None
    
    async def _clear_topic_cache_if_needed(self) -> None:
        """Очистка кэша верификации топиков по таймауту"""
        now = datetime.now()
        if (now - self._last_cache_clear).total_seconds() > self._topic_cache_timeout:
            self._topic_verification_cache.clear()
            self._last_cache_clear = now
            self.logger.debug("Cleared topic verification cache")
    
    async def _verify_topic_exists_cached(self, topic_id: int) -> bool:
        """Проверка существования топика с кэшированием"""
        # Проверяем кэш
        if topic_id in self._topic_verification_cache:
            return self._topic_verification_cache[topic_id]
        
        # Проверяем у Telegram API
        exists = await self._verify_topic_exists(topic_id)
        
        # Кэшируем результат
        self._topic_verification_cache[topic_id] = exists
        
        return exists
    
    async def _verify_topic_exists(self, topic_id: int) -> bool:
        """Verify that a topic still exists"""
        try:
            # ИСПРАВЛЕНИЕ: Упрощаем проверку
            self.bot.get_forum_topic(
                chat_id=self.settings.telegram_chat_id,
                message_thread_id=topic_id
            )
            return True
        except Exception as e:
            self.logger.debug("Topic verification failed", 
                            topic_id=topic_id, 
                            error=str(e))
            return False
    
    def setup_bot_handlers(self) -> None:
        """Setup Telegram bot command handlers"""
        
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            """Welcome message with status"""
            text = (
                f"🤖 {self.settings.app_name} v{self.settings.app_version}\n\n"
                f"🔥 ИСПРАВЛЕННАЯ версия:\n"
                f"• 1 Discord Server = 1 Telegram Topic\n"
                f"• Только announcement каналы\n"
                f"• Дедупликация сообщений\n\n"
                f"📊 Статус:\n"
                f"• Server topics: {len(self.server_topics)}\n"
                f"• Messages processed: {len(self.message_mappings)}\n"
                f"• Cache: {len(self._processed_messages)}\n\n"
                f"Команды: /status /list_topics /clean_topics"
            )
            
            self.bot.send_message(message.chat.id, text)
        
        @self.bot.message_handler(commands=['status'])
        def status_command(message):
            """Show detailed status"""
            status_text = self._get_status_text()
            self.bot.send_message(message.chat.id, status_text)
        
        @self.bot.message_handler(commands=['clean_topics'])
        def clean_topics_command(message):
            """Clean invalid topics"""
            cleaned_count = asyncio.run(self._clean_invalid_topics())
            self.bot.send_message(
                message.chat.id,
                f"🧹 Cleaned {cleaned_count} invalid topics.\n"
                f"📋 Active topics: {len(self.server_topics)}"
            )
        
        @self.bot.message_handler(commands=['list_topics'])
        def list_topics_command(message):
            """List all server topics"""
            if not self.server_topics:
                self.bot.send_message(message.chat.id, "❌ No server topics found.")
                return
            
            text = f"📋 Server Topics ({len(self.server_topics)}):\n\n"
            for server_name, topic_id in self.server_topics.items():
                text += f"• {server_name} → Topic {topic_id}\n"
            
            self.bot.send_message(message.chat.id, text)
        
        @self.bot.message_handler(commands=['debug'])
        def debug_command(message):
            """Debug information"""
            debug_text = (
                f"🔧 Debug Info:\n"
                f"• Topics: {len(self.server_topics)}\n"
                f"• Locks: {len(self._topic_creation_lock)}\n"
                f"• Cache: {len(self._topic_verification_cache)}\n"
                f"• Messages: {len(self._processed_messages)}\n"
                f"• Bot running: {self.bot_running}\n"
                f"• Use topics: {self.settings.use_topics}\n"
                f"• Chat ID: {self.settings.telegram_chat_id}"
            )
            self.bot.send_message(message.chat.id, debug_text)
    
    def _get_status_text(self) -> str:
        """Get formatted status text"""
        rate_stats = self.rate_limiter.get_stats()
        
        return (
            f"📊 Telegram Service Status:\n\n"
            f"Server Topics (1:1):\n"
            f"• Active topics: {len(self.server_topics)}\n"
            f"• Creation locks: {len(self._topic_creation_lock)}\n"
            f"• Cache size: {len(self._topic_verification_cache)}\n\n"
            f"Message Deduplication:\n"
            f"• Processed: {len(self._processed_messages)}\n"
            f"• Tracked: {len(self.message_mappings)}\n\n"
            f"Rate Limiting:\n"
            f"• Limit: {self.settings.telegram_rate_limit_per_minute}/min\n"
            f"• Success: {rate_stats.get('success_count', 0)}\n"
            f"• Errors: {rate_stats.get('error_count', 0)}\n"
        )
    
    async def _clean_invalid_topics(self) -> int:
        """Clean invalid topic mappings"""
        invalid_topics = []
        
        self.logger.info("Starting topic validation", topic_count=len(self.server_topics))
        
        for server_name, topic_id in list(self.server_topics.items()):
            if not await self._verify_topic_exists(topic_id):
                invalid_topics.append(server_name)
                self.logger.warning("Found invalid topic", 
                                  server=server_name, 
                                  topic_id=topic_id)
        
        # Remove invalid topics
        for server_name in invalid_topics:
            topic_id = self.server_topics[server_name]
            del self.server_topics[server_name]
            
            # Также очищаем из кэша верификации
            if topic_id in self._topic_verification_cache:
                del self._topic_verification_cache[topic_id]
            
            # Очищаем блокировку
            if server_name in self._topic_creation_lock:
                del self._topic_creation_lock[server_name]
            
            self.logger.info("Removed invalid topic", 
                           server=server_name,
                           topic_id=topic_id)
        
        if invalid_topics:
            await self._save_persistent_data()
            self.logger.info("Cleaned invalid topics", 
                           cleaned_count=len(invalid_topics),
                           remaining_topics=len(self.server_topics))
        
        return len(invalid_topics)
    
    def add_new_message_callback(self, callback: Callable[[DiscordMessage], None]) -> None:
        """Add callback for new messages"""
        self.new_message_callbacks.append(callback)
    
    async def start_bot_async(self) -> None:
        """Start the Telegram bot asynchronously"""
        if self.bot_running:
            self.logger.warning("Bot is already running")
            return
        
        self.setup_bot_handlers()
        self.bot_running = True
        
        self.logger.info("Starting ИСПРАВЛЕННЫЙ Telegram bot", 
                       chat_id=self.settings.telegram_chat_id,
                       use_topics=self.settings.use_topics,
                       server_topics=len(self.server_topics))
        
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, 
                lambda: self.bot.polling(
                    none_stop=True,
                    interval=1,
                    timeout=30,
                    skip_pending=True
                )
            )
        except Exception as e:
            self.logger.error("Bot polling error", error=str(e))
        finally:
            self.bot_running = False
            self.logger.info("ИСПРАВЛЕННЫЙ Telegram bot stopped")
    
    def stop_bot(self) -> None:
        """Stop the Telegram bot"""
        if self.bot_running:
            try:
                self.bot.stop_polling()
                self.bot_running = False
                self.logger.info("Telegram bot stopped")
            except Exception as e:
                self.logger.error("Error stopping bot", error=str(e))
                self.bot_running = False
    
    async def cleanup(self) -> None:
        """Clean up resources"""
        self.stop_bot()
        await self._save_persistent_data()
        
        # Очистка кэшей
        async with self._message_dedup_lock:
            self._processed_messages.clear()
        
        self._topic_verification_cache.clear()
        self._topic_creation_lock.clear()
        
        self.logger.info("ИСПРАВЛЕННЫЙ Telegram service cleaned up",
                        final_topics=len(self.server_topics))