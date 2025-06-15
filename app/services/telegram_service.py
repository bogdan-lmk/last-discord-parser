# app/services/telegram_service.py
import asyncio
import json
from datetime import datetime
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
    """Clean Telegram service with dependency injection and proper error handling"""
    
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
        
        # State management
        self.server_topics: Dict[str, int] = {}  # server_name -> topic_id
        self._async_lock = asyncio.Lock()
        self.user_states: Dict[int, dict] = {}  # user_id -> state
        
        # Message tracking
        self.message_mappings: Dict[str, int] = {}  # timestamp -> telegram_message_id
        
        # Callbacks
        self.new_message_callbacks: List[Callable[[DiscordMessage], None]] = []
        
        # Bot running state
        self.bot_running = False
        
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
            
            # Verify chat access
            if await self._verify_chat_access():
                self.logger.info("Chat access verified", 
                               chat_id=self.settings.telegram_chat_id)
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
        """Send a Discord message to Telegram"""
        try:
            await self.rate_limiter.wait_if_needed("telegram_send")
            
            # Get or create topic for server
            topic_id = await self._get_or_create_topic(message.server_name)
            
            # Format message
            formatted_message = message.to_telegram_format(
                show_timestamp=self.settings.show_timestamps,
                show_server=self.settings.show_server_in_message
            )
            
            # Send message
            sent_message = self.bot.send_message(
                chat_id=self.settings.telegram_chat_id,
                text=formatted_message,
                message_thread_id=topic_id if self.settings.use_topics else None,
                parse_mode='Markdown'
            )
            
            # Track message
            if sent_message:
                self.message_mappings[str(message.timestamp)] = sent_message.message_id
                await self._save_persistent_data()
                
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
    
    async def send_messages_batch(self, messages: List[DiscordMessage]) -> int:
        """Send multiple messages as a batch"""
        if not messages:
            return 0
        
        # Group messages by server
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
            
            # Send each message
            for message in server_messages:
                if await self.send_message(message):
                    sent_count += 1
                
                # Rate limiting between messages
                await asyncio.sleep(0.1)
        
        self.logger.info("Batch sending complete", 
                       total_messages=len(messages),
                       sent_messages=sent_count)
        
        return sent_count
    
    async def _get_or_create_topic(self, server_name: str) -> Optional[int]:
        """Get existing topic or create new one for server - ИСПРАВЛЕНО: убрано дублирование"""
        if not self.settings.use_topics:
            return None
        
        async with self._async_lock:
            # Check cache first
            if server_name in self.server_topics:
                topic_id = self.server_topics[server_name]
                
                # Verify topic still exists
                if await self._verify_topic_exists(topic_id):
                    return topic_id
                else:
                    # Topic was deleted, remove from cache
                    del self.server_topics[server_name]
            
            # Create new topic if needed (ИСПРАВЛЕНО: только один блок создания)
            try:
                topic = self.bot.create_forum_topic(
                    chat_id=self.settings.telegram_chat_id,
                    name=f"🏰 {server_name}",
                    icon_color=0x6FB9F0
                )
                
                topic_id = topic.message_thread_id
                self.server_topics[server_name] = topic_id
                
                # Save to persistent storage
                asyncio.create_task(self._save_persistent_data())
                
                self.logger.info("Created new topic", 
                               server=server_name,
                               topic_id=topic_id)
                
                return topic_id
                
            except Exception as e:
                self.logger.error("Failed to create topic", 
                                server=server_name,
                                error=str(e))
                return None
    
    async def _verify_topic_exists(self, topic_id: int) -> bool:
        """Verify that a topic still exists"""
        try:
            self.bot.get_forum_topic(
                chat_id=self.settings.telegram_chat_id,
                message_thread_id=topic_id
            )
            return True
        except Exception:
            return False
    
    def setup_bot_handlers(self) -> None:
        """Setup Telegram bot command handlers"""
        
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            """Welcome message with status"""
            text = (
                f"🤖 **{self.settings.app_name} v{self.settings.app_version}**\n\n"
                f"🔥 **Features:**\n"
                f"• Real-time Discord monitoring\n"
                f"• Smart topic management\n" 
                f"• Rate limiting protection\n"
                f"• Professional error handling\n\n"
                f"📊 **Current Status:**\n"
                f"• Topics: {len(self.server_topics)}\n"
                f"• Messages processed: {len(self.message_mappings)}\n"
                f"• Rate limiter: {self.rate_limiter.name}\n\n"
                f"Use the buttons below to interact:"
            )
            
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("📊 Status", callback_data="status"),
                InlineKeyboardButton("📋 Servers", callback_data="servers"),
                InlineKeyboardButton("🔧 Settings", callback_data="settings"),
                InlineKeyboardButton("❓ Help", callback_data="help")
            )
            
            self.bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode='Markdown')
        
        @self.bot.callback_query_handler(func=lambda call: True)
        def handle_callback_query(call):
            """Handle callback queries"""
            try:
                data = call.data
                self.logger.info("Callback received", data=data, user_id=call.from_user.id)
                
                if data == "status":
                    self._handle_status_callback(call)
                elif data == "servers":
                    self._handle_servers_callback(call)
                elif data == "settings":
                    self._handle_settings_callback(call)
                elif data == "help":
                    self._handle_help_callback(call)
                else:
                    self.bot.answer_callback_query(call.id, "Unknown command")
                    
            except Exception as e:
                self.logger.error("Error handling callback", error=str(e))
                self.bot.answer_callback_query(call.id, "Error occurred")
        
        @self.bot.message_handler(commands=['status'])
        def status_command(message):
            """Show detailed status"""
            status_text = self._get_status_text()
            self.bot.send_message(message.chat.id, status_text, parse_mode='Markdown')
        
        @self.bot.message_handler(commands=['clean_topics'])
        def clean_topics_command(message):
            """Clean invalid topics"""
            cleaned_count = asyncio.run(self._clean_invalid_topics())
            self.bot.send_message(
                message.chat.id,
                f"🧹 Cleaned {cleaned_count} invalid topics.\n"
                f"📋 Active topics: {len(self.server_topics)}"
            )
    
    def _handle_status_callback(self, call):
        """Handle status callback"""
        status_text = self._get_status_text()
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="start"))
        
        self.bot.edit_message_text(
            status_text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode='Markdown'
        )
        self.bot.answer_callback_query(call.id)
    
    def _handle_servers_callback(self, call):
        """Handle servers callback"""
        if not self.server_topics:
            text = "❌ No servers configured yet."
        else:
            text = f"📋 **Configured Servers** ({len(self.server_topics)}):\n\n"
            for server_name, topic_id in self.server_topics.items():
                topic_status = "✅" if asyncio.run(self._verify_topic_exists(topic_id)) else "❌"
                text += f"• {server_name} - Topic {topic_id} {topic_status}\n"
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="start"))
        
        self.bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode='Markdown'
        )
        self.bot.answer_callback_query(call.id)
    
    def _handle_settings_callback(self, call):
        """Handle settings callback"""
        text = (
            f"⚙️ **Current Settings:**\n\n"
            f"• Use topics: {self.settings.use_topics}\n"
            f"• Show timestamps: {self.settings.show_timestamps}\n"
            f"• Show server in message: {self.settings.show_server_in_message}\n"
            f"• Max channels per server: {self.settings.max_channels_per_server}\n"
            f"• Max total channels: {self.settings.max_total_channels}\n"
            f"• Rate limit (Telegram): {self.settings.telegram_rate_limit_per_minute}/min\n"
        )
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="start"))
        
        self.bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode='Markdown'
        )
        self.bot.answer_callback_query(call.id)
    
    def _handle_help_callback(self, call):
        """Handle help callback"""
        text = (
            f"❓ **Help & Commands:**\n\n"
            f"**Bot Commands:**\n"
            f"• `/start` - Show main menu\n"
            f"• `/status` - Show detailed status\n"
            f"• `/clean_topics` - Clean invalid topics\n\n"
            f"**Features:**\n"
            f"• Automatic topic creation for each Discord server\n"
            f"• Real-time message forwarding\n"
            f"• Rate limiting protection\n"
            f"• Error recovery and retry logic\n"
            f"• Persistent state management\n\n"
            f"**Support:**\n"
            f"• Check logs for detailed information\n"
            f"• Topics are automatically managed\n"
            f"• Bot recovers from temporary failures\n"
        )
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="start"))
        
        self.bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode='Markdown'
        )
        self.bot.answer_callback_query(call.id)
    
    def _get_status_text(self) -> str:
        """Get formatted status text"""
        rate_stats = self.rate_limiter.get_stats()
        
        return (
            f"📊 **Telegram Service Status:**\n\n"
            f"**Topics:**\n"
            f"• Active topics: {len(self.server_topics)}\n"
            f"• Topics enabled: {self.settings.use_topics}\n\n"
            f"**Messages:**\n"
            f"• Messages tracked: {len(self.message_mappings)}\n"
            f"• Show timestamps: {self.settings.show_timestamps}\n\n"
            f"**Rate Limiting:**\n"
            f"• Limit: {self.settings.telegram_rate_limit_per_minute}/min\n"
            f"• Success rate: {rate_stats.get('success_count', 0)}\n"
            f"• Error count: {rate_stats.get('error_count', 0)}\n"
            f"• Adaptive multiplier: {rate_stats.get('adaptive_multiplier', 1.0):.2f}\n\n"
            f"**Storage:**\n"
            f"• Redis: {'✅' if self.redis_client else '❌'}\n"
            f"• Cache TTL: {self.settings.cache_ttl_seconds}s\n"
        )
    
    async def _clean_invalid_topics(self) -> int:
        """Clean invalid topic mappings"""
        invalid_topics = []
        
        for server_name, topic_id in list(self.server_topics.items()):
            if not await self._verify_topic_exists(topic_id):
                invalid_topics.append(server_name)
        
        # Remove invalid topics
        for server_name in invalid_topics:
            del self.server_topics[server_name]
            self.logger.info("Removed invalid topic", 
                           server=server_name,
                           topic_id=self.server_topics.get(server_name))
        
        if invalid_topics:
            await self._save_persistent_data()
        
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
        
        self.logger.info("Starting Telegram bot", 
                       chat_id=self.settings.telegram_chat_id,
                       use_topics=self.settings.use_topics)
        
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
            self.logger.info("Telegram bot stopped")
    
    # ИСПРАВЛЕНИЕ: Добавляем отсутствующий метод stop_bot
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
        self.logger.info("Telegram service cleaned up")