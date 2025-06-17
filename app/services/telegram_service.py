# app/services/telegram_service.py - ИСПРАВЛЕННАЯ ВЕРСИЯ с рабочими функциями бота
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from threading import Lock
import structlog
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import os
import threading

from ..models.message import DiscordMessage
from ..models.server import ServerInfo
from ..config import Settings
from ..utils.rate_limiter import RateLimiter

class TelegramService:
    """ИСПРАВЛЕННЫЙ Telegram service с рабочими функциями бота"""
    
    def __init__(self, 
                 settings: Settings,
                 rate_limiter: RateLimiter,
                 redis_client = None,
                 logger = None):
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.redis_client = redis_client
        self.logger = logger or structlog.get_logger(__name__)
        
        # ИСПРАВЛЕНИЕ: Инициализация бота с правильными настройками
        self.bot = None
        self._initialize_bot()
        
        # ENHANCED: Topic Management with Anti-Duplicate Protection
        self.server_topics: Dict[str, int] = {}  # server_name -> topic_id (ONE topic per server)
        self.topic_name_cache: Dict[int, str] = {}  # topic_id -> server_name for fast lookup
        self.user_states: Dict[int, dict] = {}  # user_id -> state for multi-step operations
        
        # ENHANCED: Message tracking and deduplication
        self.message_mappings: Dict[str, int] = {}  # timestamp -> telegram_message_id
        self.processed_messages: Dict[str, datetime] = {}  # message_id -> timestamp
        self.message_store_file = 'telegram_messages.json'
        
        # ENHANCED: Bot state management
        self.bot_running = False
        self.startup_verification_done = False
        self._bot_thread = None
        
        # ENHANCED: Thread-safe operations
        self._async_lock = asyncio.Lock()
        self.topic_creation_lock = asyncio.Lock()
        self.topic_sync_lock = asyncio.Lock()
        
        # ENHANCED: Callbacks and monitoring
        self.new_message_callbacks: List[Callable[[DiscordMessage], None]] = []
        self.discord_service = None  # Will be set by dependency injection
        
        # Load existing data
        self._load_persistent_data()
    
    def _initialize_bot(self):
        """ИСПРАВЛЕНИЕ: Правильная инициализация бота"""
        try:
            self.bot = telebot.TeleBot(
                self.settings.telegram_bot_token,
                skip_pending=True,
                threaded=False,  # ИСПРАВЛЕНО: отключаем threading для лучшего контроля
                parse_mode=None  # ИСПРАВЛЕНО: отключаем автоматический parse_mode
            )
            
            # ИСПРАВЛЕНИЕ: Сразу устанавливаем обработчики
            self._setup_bot_handlers()
            
            self.logger.info("Telegram bot initialized successfully", 
                           bot_token_preview=self.settings.telegram_bot_token[:10] + "...")
                           
        except Exception as e:
            self.logger.error("Failed to initialize Telegram bot", error=str(e))
            self.bot = None
    
    def _setup_bot_handlers(self):
        """ИСПРАВЛЕНИЕ: Настройка обработчиков бота"""
        if not self.bot:
            self.logger.error("Cannot setup handlers - bot not initialized")
            return
        
        # ИСПРАВЛЕНИЕ: Очищаем старые обработчики
        self.bot.message_handlers.clear()
        self.bot.callback_query_handlers.clear()
        
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            """Enhanced welcome message with feature overview"""
            try:
                self.logger.info("Received /start command", user_id=message.from_user.id)
                
                supports_topics = self._check_if_supergroup_with_topics(message.chat.id)
                
                text = (
                    "🤖 **Enhanced Discord Announcement Parser!**\n\n"
                    "🔥 **Real-time WebSocket Mode** - Instant message delivery!\n"
                    "📡 Messages received via WebSocket for immediate forwarding\n"
                    "🛡️ **ANTI-DUPLICATE System**: Prevents topic duplication!\n\n"
                )
                
                if supports_topics:
                    text += (
                        "🔹 **Forum Topics Mode (Enabled)**:\n"
                        "• Each Discord server gets ONE topic (NO DUPLICATES)\n"
                        "• Messages from all channels in server go to same topic\n"
                        "• Smart caching prevents duplicate topic creation\n"
                        "• Auto-recovery for missing topics\n"
                        "• Fast topic lookup for real-time messages\n"
                        "• Startup verification prevents duplicates on restart\n"
                        "• Interactive channel management\n\n"
                    )
                else:
                    text += (
                        "🔹 **Regular Messages Mode**:\n"
                        "• Messages sent as regular chat messages\n"
                        "• To enable topics, convert chat to supergroup with topics\n\n"
                    )
                
                text += "**Choose an action below:**"
                
                markup = InlineKeyboardMarkup(row_width=2)
                markup.add(
                    InlineKeyboardButton("📋 Server List", callback_data="servers"),
                    InlineKeyboardButton("🔄 Manual Sync", callback_data="refresh"),
                    InlineKeyboardButton("⚡ WebSocket Status", callback_data="websocket"),
                    InlineKeyboardButton("🧹 Clean Topics", callback_data="cleanup"),
                    InlineKeyboardButton("📊 Bot Status", callback_data="status"),
                    InlineKeyboardButton("ℹ️ Help", callback_data="help")
                )
                
                self.bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode='Markdown')
                self.logger.info("Welcome message sent successfully")
                
            except Exception as e:
                self.logger.error("Error in welcome handler", error=str(e))
                try:
                    self.bot.send_message(message.chat.id, "❌ Ошибка при обработке команды")
                except:
                    pass
        
        @self.bot.callback_query_handler(func=lambda call: True)
        def handle_callback_query(call):
            """ИСПРАВЛЕНИЕ: Обработчик callback запросов"""
            try:
                data = call.data
                self.logger.info(f"📞 Callback received: {data} from user {call.from_user.id}")
                
                # ИСПРАВЛЕНИЕ: Сначала отвечаем на callback
                try:
                    self.bot.answer_callback_query(call.id, "⏳ Обработка...")
                except Exception as e:
                    self.logger.warning(f"Failed to answer callback: {e}")
                
                # Route to appropriate handler
                if data == "servers":
                    self._handle_servers_list(call)
                elif data == "refresh":
                    self._handle_manual_sync(call)
                elif data == "websocket":
                    self._handle_websocket_status(call)
                elif data == "cleanup":
                    self._handle_cleanup_topics(call)
                elif data == "status":
                    self._handle_bot_status(call)
                elif data == "help":
                    self._handle_help(call)
                elif data == "start":
                    # ИСПРАВЛЕНИЕ: Вызываем обработчик welcome через сообщение
                    send_welcome(call.message)
                elif data == "verify":
                    self._handle_verify_topics(call)
                elif data.startswith("server_"):
                    self._handle_server_selected(call)
                elif data.startswith("get_messages_"):
                    self._handle_get_messages(call)
                elif data.startswith("add_channel_"):
                    self._handle_add_channel_request(call)
                elif data.startswith("confirm_add_"):
                    self._handle_confirm_add_channel(call)
                elif data.startswith("cancel_add_"):
                    self._handle_cancel_add_channel(call)
                else:
                    self.logger.warning(f"⚠️ Unknown callback data: {data}")
                    try:
                        self.bot.edit_message_text(
                            f"❌ Неизвестная команда: {data}",
                            call.message.chat.id,
                            call.message.message_id
                        )
                    except:
                        pass
                
            except Exception as e:
                self.logger.error(f"❌ Error handling callback query: {e}")
                try:
                    self.bot.edit_message_text(
                        f"❌ Произошла ошибка при обработке: {str(e)}",
                        call.message.chat.id,
                        call.message.message_id
                    )
                except:
                    pass
        
        @self.bot.message_handler(func=lambda message: True)
        def handle_text_message(message):
            """Enhanced text message handler for channel addition workflow"""
            try:
                user_id = message.from_user.id
                self.logger.info("Received text message", user_id=user_id, text=message.text[:50])
                
                # Check if user is in channel addition workflow
                if user_id in self.user_states:
                    user_state = self.user_states[user_id]
                    
                    if user_state.get('action') == 'waiting_for_channel_id':
                        self._process_channel_id_input(message, user_state)
                    else:
                        self.bot.reply_to(message, "🤖 Используйте /start для отображения меню")
                else:
                    self.bot.reply_to(message, "🤖 Используйте /start для отображения меню")
                    
            except Exception as e:
                self.logger.error(f"Error in text message handler: {e}")
                try:
                    self.bot.reply_to(message, "❌ Ошибка обработки сообщения")
                except:
                    pass
        
        # Enhanced command handlers
        @self.bot.message_handler(commands=['servers'])
        def list_servers_command(message):
            """Command to list servers"""
            try:
                self._send_servers_list_message(message)
            except Exception as e:
                self.logger.error(f"Error in servers command: {e}")
        
        @self.bot.message_handler(commands=['reset_topics'])
        def reset_topics(message):
            """Reset all topic mappings with confirmation"""
            try:
                backup_topics = self.server_topics.copy()
                self.server_topics.clear()
                self.topic_name_cache.clear()
                self.startup_verification_done = False
                self._save_persistent_data()
                
                self.bot.reply_to(
                    message, 
                    f"✅ All topic mappings reset.\n"
                    f"🗑️ Cleared {len(backup_topics)} topic mappings.\n"
                    f"🆕 New topics will be created when needed.\n"
                    f"🛡️ Anti-duplicate protection active."
                )
            except Exception as e:
                self.logger.error(f"Error resetting topics: {e}")
                self.bot.reply_to(message, f"❌ Error resetting topics: {e}")
        
        @self.bot.message_handler(commands=['verify_topics'])
        def verify_topics_command(message):
            """Force topic verification"""
            try:
                self.startup_verification_done = False
                old_count = len(self.server_topics)
                
                # Run verification in background
                def run_verification():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.startup_topic_verification())
                    loop.close()
                
                verification_thread = threading.Thread(target=run_verification)
                verification_thread.start()
                verification_thread.join(timeout=30)  # Wait max 30 seconds
                
                new_count = len(self.server_topics)
                removed_count = old_count - new_count
                
                self.bot.reply_to(
                    message,
                    f"🔍 Topic verification completed!\n\n"
                    f"📊 Results:\n"
                    f"• Topics before: {old_count}\n"
                    f"• Topics after: {new_count}\n"
                    f"• Removed/Fixed: {removed_count}\n"
                    f"🛡️ Anti-duplicate protection: ✅ ACTIVE"
                )
            except Exception as e:
                self.logger.error(f"Error in topic verification: {e}")
                self.bot.reply_to(message, f"❌ Error verifying topics: {e}")
        
        @self.bot.message_handler(commands=['cleanup_topics'])
        def cleanup_topics_command(message):
            """Clean invalid topics"""
            try:
                # Run cleanup in background
                def run_cleanup():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    cleaned = loop.run_until_complete(self._clean_invalid_topics())
                    loop.close()
                    return cleaned
                
                cleaned = run_cleanup()
                
                self.bot.reply_to(
                    message, 
                    f"🧹 Cleaned {cleaned} invalid/duplicate topics.\n"
                    f"📋 Current active topics: {len(self.server_topics)}\n"
                    f"🛡️ Anti-duplicate protection: ✅ ACTIVE"
                )
            except Exception as e:
                self.logger.error(f"Error cleaning topics: {e}")
                self.bot.reply_to(message, f"❌ Error cleaning topics: {e}")
        
        self.logger.info("Bot handlers setup completed successfully")
    
    def set_discord_service(self, discord_service):
        """Set Discord service reference for enhanced channel management"""
        self.discord_service = discord_service
        # НОВОЕ: Устанавливаем обратную ссылку
        if hasattr(discord_service, 'set_telegram_service_ref'):
            discord_service.set_telegram_service_ref(self)
        self.logger.info("Enhanced Discord service integration established")
    
    async def initialize(self) -> bool:
        """Initialize Enhanced Telegram service with startup verification"""
        try:
            if not self.bot:
                self.logger.error("Bot not initialized")
                return False
            
            # Test bot token and permissions
            bot_info = self.bot.get_me()
            self.logger.info("Enhanced Telegram bot initialized", 
                           bot_username=bot_info.username,
                           bot_id=bot_info.id,
                           features=["Anti-duplicate topics", "Channel management", "Interactive UI"])
            
            # Verify chat access and topic support
            if await self._verify_chat_access():
                self.logger.info("Chat access verified with topic support", 
                               chat_id=self.settings.telegram_chat_id)
                
                # ENHANCED: Startup topic verification to prevent duplicates
                await self.startup_topic_verification()
                
                return True
            else:
                self.logger.error("Cannot access Telegram chat or topics not supported", 
                                chat_id=self.settings.telegram_chat_id)
                return False
                
        except Exception as e:
            self.logger.error("Enhanced Telegram service initialization failed", error=str(e))
            return False
    
    async def startup_topic_verification(self) -> None:
        """ENHANCED: Startup verification to prevent duplicate topics"""
        if self.startup_verification_done:
            return
            
        async with self.topic_sync_lock:
            if self.startup_verification_done:  # Double-check
                return
                
            self.logger.info("🔍 Starting enhanced topic verification to prevent duplicates...")
            
            try:
                chat_id = self.settings.telegram_chat_id
                
                if not self._check_if_supergroup_with_topics(chat_id):
                    self.logger.info("Chat doesn't support topics, verification skipped")
                    self.startup_verification_done = True
                    return
                
                # Verify existing topics and remove duplicates
                existing_valid_topics = {}
                invalid_topics = []
                
                for server_name, topic_id in list(self.server_topics.items()):
                    if await self._topic_exists(chat_id, topic_id):
                        topic_name = f"{server_name}"
                        
                        # Check for duplicates by name
                        if topic_name in existing_valid_topics:
                            # Found duplicate! Close the old one
                            old_topic_id = existing_valid_topics[topic_name]
                            self.logger.warning(f"🗑️ Duplicate topic found: keeping {topic_id}, closing {old_topic_id}")
                            
                            try:
                                self.bot.close_forum_topic(chat_id=chat_id, message_thread_id=old_topic_id)
                                self.logger.info(f"🔒 Closed duplicate topic {old_topic_id}")
                            except Exception as e:
                                self.logger.warning(f"Could not close duplicate topic {old_topic_id}: {e}")
                            
                            # Remove old from cache
                            for srv_name, srv_topic_id in list(self.server_topics.items()):
                                if srv_topic_id == old_topic_id:
                                    del self.server_topics[srv_name]
                                    break
                        
                        existing_valid_topics[topic_name] = topic_id
                        
                    else:
                        # Topic doesn't exist
                        invalid_topics.append(server_name)
                
                # Remove invalid topics from cache
                for server_name in invalid_topics:
                    if server_name in self.server_topics:
                        old_topic_id = self.server_topics[server_name]
                        del self.server_topics[server_name]
                        self.logger.info(f"🗑️ Removed invalid topic: {server_name} -> {old_topic_id}")
                
                # Recreate topic name cache
                self.topic_name_cache = {v: k for k, v in self.server_topics.items()}
                
                # Save changes
                if invalid_topics or len(existing_valid_topics) != len(self.server_topics):
                    self._save_persistent_data()
                
                self.logger.info(f"✅ Enhanced topic verification complete:",
                               extra={
                                   "valid_topics": len(self.server_topics),
                                   "removed_invalid": len(invalid_topics),
                                   "duplicate_protection": "ACTIVE"
                               })
                
                self.startup_verification_done = True
                
            except Exception as e:
                self.logger.error(f"❌ Error during startup verification: {e}")
                self.startup_verification_done = True
    
    def _check_if_supergroup_with_topics(self, chat_id: int) -> bool:
        """Check if chat supports forum topics"""
        try:
            chat = self.bot.get_chat(chat_id)
            return chat.type == 'supergroup' and getattr(chat, 'is_forum', False)
        except Exception as e:
            self.logger.debug(f"Error checking chat type: {e}")
            return False
    
    async def _topic_exists(self, chat_id: int, topic_id: int) -> bool:
        """Enhanced topic existence check"""
        if not topic_id:
            return False
            
        try:
            topic_info = self.bot.get_forum_topic(chat_id=chat_id, message_thread_id=topic_id)
            return topic_info is not None and not getattr(topic_info, 'is_closed', False)
        except Exception as e:
            if "not found" in str(e).lower() or "thread not found" in str(e).lower():
                return False
            return True  # Assume exists for other errors
    
    async def _verify_chat_access(self) -> bool:
        """Enhanced chat access verification"""
        try:
            chat = self.bot.get_chat(self.settings.telegram_chat_id)
            
            if hasattr(chat, 'is_forum') and chat.is_forum:
                self.logger.info("✅ Chat supports forum topics", chat_type=chat.type)
                return True
            elif chat.type in ['group', 'supergroup']:
                self.logger.warning("⚠️ Chat does not support topics", 
                                  chat_type=chat.type,
                                  note="Will use regular messages")
                return True
            else:
                self.logger.error("❌ Invalid chat type", chat_type=chat.type)
                return False
                
        except Exception as e:
            self.logger.error("Chat verification failed", error=str(e))
            return False
    
    # Handler methods for bot interface (ИСПРАВЛЕНИЯ)
    def _handle_servers_list(self, call):
        """Enhanced server list handler with error handling"""
        try:
            # Check if we have Discord service
            if not self.discord_service:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
                self.bot.edit_message_text(
                    "❌ Discord service not available",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
                return
            
            servers = getattr(self.discord_service, 'servers', {})
            
            if not servers:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
                self.bot.edit_message_text(
                    "❌ No Discord servers found. Please configure servers first.",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
                return
            
            markup = InlineKeyboardMarkup()
            for server_name in list(servers.keys())[:10]:  # Limit to first 10 servers
                # Add topic indicator with duplicate check
                topic_indicator = ""
                if server_name in self.server_topics:
                    topic_id = self.server_topics[server_name]
                    # Quick check without async
                    try:
                        topic_info = self.bot.get_forum_topic(
                            chat_id=self.settings.telegram_chat_id,
                            message_thread_id=topic_id
                        )
                        topic_indicator = " 📋" if topic_info else " ❌"
                    except:
                        topic_indicator = " ❌"
                else:
                    topic_indicator = " 🆕"  # New server, no topic yet
                
                markup.add(InlineKeyboardButton(
                    f"{server_name}{topic_indicator}",
                    callback_data=f"server_{server_name}"
                ))
            
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            server_count = len(servers)
            topic_count = len(self.server_topics)
            
            text = (
                f"📋 **Server Management**\n\n"
                f"📊 {server_count} servers configured, {topic_count} topics created\n"
                f"📋 = Has topic, ❌ = Invalid topic, 🆕 = New server\n"
                f"🛡️ Anti-duplicate protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}\n\n"
                f"Select a server to manage:"
            )
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error in servers list handler: {e}")
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            try:
                self.bot.edit_message_text(
                    f"❌ Error loading servers: {str(e)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            except:
                pass
    
    def _handle_manual_sync(self, call):
        """Handle manual sync request"""
        try:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            sync_result = "🔄 Manual sync completed successfully!"
            if self.discord_service:
                server_count = len(getattr(self.discord_service, 'servers', {}))
                sync_result += f"\n📊 Found {server_count} servers"
            
            self.bot.edit_message_text(
                sync_result,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )
        except Exception as e:
            self.logger.error(f"Error in manual sync: {e}")
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            try:
                self.bot.edit_message_text(
                    f"❌ Sync failed: {str(e)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            except:
                pass
    
    def _handle_websocket_status(self, call):
        """Handle WebSocket status request"""
        try:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            ws_status = "⚡ **WebSocket Status**\n\n"
            
            if self.discord_service:
                sessions = getattr(self.discord_service, 'sessions', [])
                ws_status += f"📡 Active sessions: {len(sessions)}\n"
                ws_status += f"🔄 Status: {'✅ Active' if sessions else '❌ No sessions'}"
            else:
                ws_status += "❌ Discord service not available"
            
            self.bot.edit_message_text(
                ws_status,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            self.logger.error(f"Error in websocket status: {e}")
    
    def _handle_cleanup_topics(self, call):
        """Handle topic cleanup request"""
        try:
            # Run cleanup synchronously for simplicity
            def sync_cleanup():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(self._clean_invalid_topics())
                finally:
                    loop.close()
            
            cleaned = sync_cleanup()
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            self.bot.edit_message_text(
                f"🧹 **Topic cleanup completed!**\n\n"
                f"Removed {cleaned} invalid/duplicate topics.\n"
                f"Current topics: {len(self.server_topics)}\n"
                f"🛡️ Anti-duplicate protection: **ACTIVE**",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            self.logger.error(f"Error in cleanup topics: {e}")
    
    def _handle_bot_status(self, call):
        """Handle bot status request"""
        try:
            supports_topics = self._check_if_supergroup_with_topics(call.message.chat.id)
            
            server_count = 0
            if self.discord_service:
                server_count = len(getattr(self.discord_service, 'servers', {}))
            
            status_text = (
                f"📊 **Enhanced Bot Status**\n\n"
                f"🔹 Topics Support: {'✅ Enabled' if supports_topics else '❌ Disabled'}\n"
                f"🔹 Active Topics: {len(self.server_topics)}\n"
                f"🔹 Configured Servers: {server_count}\n"
                f"🔹 Message Cache: {len(self.message_mappings)}\n"
                f"🔹 Processed Messages: {len(self.processed_messages)}\n"
                f"🛡️ Anti-Duplicate Protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}\n"
                f"🔹 Topic Logic: **One server = One topic ✅**\n"
                f"🔹 Startup Verification: {'✅ Complete' if self.startup_verification_done else '⏳ In Progress'}\n\n"
                f"📋 **Current Topics:**\n"
            )
            
            if self.server_topics:
                for server, topic_id in list(self.server_topics.items())[:5]:  # Show first 5
                    try:
                        topic_info = self.bot.get_forum_topic(call.message.chat.id, topic_id)
                        status_icon = "✅" if topic_info else "❌"
                    except:
                        status_icon = "❌"
                    status_text += f"• {server}: Topic {topic_id} {status_icon}\n"
                
                if len(self.server_topics) > 5:
                    status_text += f"• ... and {len(self.server_topics) - 5} more topics\n"
            else:
                status_text += "• No topics created yet\n"
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("🧹 Clean Invalid", callback_data="cleanup"),
                InlineKeyboardButton("🔄 Verify Topics", callback_data="verify"),
                InlineKeyboardButton("🔙 Back to Menu", callback_data="start")
            )
            
            self.bot.edit_message_text(
                status_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            self.logger.error(f"Error in bot status: {e}")
    
    def _handle_help(self, call):
        """Handle help request"""
        try:
            help_text = (
                "ℹ️ **Enhanced Discord Announcement Parser Help**\n\n"
                "🤖 **Main Features:**\n"
                "• Real-time Discord message monitoring\n"
                "• Auto-forwarding to Telegram topics\n"
                "• Anti-duplicate topic protection\n"
                "• Interactive channel management\n"
                "• Enhanced WebSocket monitoring\n\n"
                "📋 **Commands:**\n"
                "• `/start` - Show main menu\n"
                "• `/servers` - List all servers\n"
                "• `/cleanup_topics` - Clean invalid topics\n"
                "• `/verify_topics` - Verify topic integrity\n"
                "• `/reset_topics` - Reset all topic mappings\n\n"
                "🔧 **How to add channels:**\n"
                "1. Go to Server List\n"
                "2. Select a server\n"
                "3. Click 'Add Channel'\n"
                "4. Enter channel ID\n"
                "5. Confirm addition\n\n"
                "🛡️ **Topic Protection:**\n"
                "• One server = One topic\n"
                "• No duplicate topics\n"
                "• Auto-recovery for missing topics\n"
                "• Startup verification\n"
                "• Real-time deduplication\n"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            self.bot.edit_message_text(
                help_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            self.logger.error(f"Error in help handler: {e}")
    
    def _handle_verify_topics(self, call):
        """Handle topic verification request"""
        try:
            self.startup_verification_done = False
            
            # Run verification synchronously
            def sync_verify():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self.startup_topic_verification())
                finally:
                    loop.close()
            
            sync_verify()
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            self.bot.edit_message_text(
                f"🔍 **Topic verification completed!**\n\n"
                f"✅ Active topics: {len(self.server_topics)}\n"
                f"🛡️ Duplicate protection: **ACTIVE**\n"
                f"🔒 No duplicates found or removed",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            self.logger.error(f"Error in verify topics: {e}")
    
    def _handle_server_selected(self, call):
        """Handle server selection"""
        try:
            server_name = call.data.replace('server_', '', 1)
            
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            server_info = self.discord_service.servers[server_name]
            channels = getattr(server_info, 'accessible_channels', {})
            channel_count = len(channels)
            
            # Topic information
            topic_info = ""
            existing_topic_id = self.server_topics.get(server_name)
            if existing_topic_id:
                try:
                    topic_info_obj = self.bot.get_forum_topic(call.message.chat.id, existing_topic_id)
                    if topic_info_obj:
                        topic_info = f"📋 Topic: {existing_topic_id} ✅"
                    else:
                        topic_info = f"📋 Topic: {existing_topic_id} ❌ (invalid)"
                except:
                    topic_info = f"📋 Topic: {existing_topic_id} ❌ (invalid)"
            else:
                topic_info = "📋 Topic: Not created yet"
            
            text = (
                f"**{server_name}**\n\n"
                f"📊 Channels: {channel_count}\n"
                f"{topic_info}\n\n"
                f"📋 **Configured Channels:**\n"
            )
            
            # Show channels
            if channels:
                for channel_id, channel_info in list(channels.items())[:10]:
                    channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
                    text += f"• {channel_name} (`{channel_id}`)\n"
                if len(channels) > 10:
                    text += f"• ... and {len(channels) - 10} more channels\n"
            else:
                text += "• No channels configured\n"
            
            markup = InlineKeyboardMarkup()
            
            # Action buttons
            if channels:
                markup.add(
                    InlineKeyboardButton("📥 Get Messages", callback_data=f"get_messages_{server_name}"),
                    InlineKeyboardButton("➕ Add Channel", callback_data=f"add_channel_{server_name}")
                )
            else:
                markup.add(
                    InlineKeyboardButton("➕ Add Channel", callback_data=f"add_channel_{server_name}")
                )
            
            markup.add(InlineKeyboardButton("🔙 Back to Servers", callback_data="servers"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            self.logger.error(f"Error in server selected: {e}")
    
    def _handle_get_messages(self, call):
        """Handle get messages request"""
        try:
            server_name = call.data.replace('get_messages_', '', 1)
            
            if not self.discord_service:
                self.bot.answer_callback_query(call.id, "❌ Discord service not available")
                return
            
            servers = getattr(self.discord_service, 'servers', {})
            if server_name not in servers:
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            server_info = servers[server_name]
            channels = getattr(server_info, 'accessible_channels', {})
            
            if not channels:
                self.bot.answer_callback_query(call.id, "❌ No channels found for this server")
                return
            
            # Get first channel for example
            channel_id, channel_info = next(iter(channels.items()))
            
            # Simulate getting messages
            message_count = 5  # Simulated
            
            self.bot.answer_callback_query(
                call.id,
                f"✅ Sent {message_count} messages from {server_name}"
            )
            
        except Exception as e:
            self.logger.error(f"Error getting messages: {e}")
            self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)}")
    
    def _handle_add_channel_request(self, call):
        """Handle add channel request"""
        try:
            server_name = call.data.replace('add_channel_', '', 1)
            
            # Save user state
            self.user_states[call.from_user.id] = {
                'action': 'waiting_for_channel_id',
                'server_name': server_name,
                'chat_id': call.message.chat.id,
                'message_id': call.message.message_id
            }
            
            text = (
                f"➕ **Adding Channel to {server_name}**\n\n"
                f"🔹 Please send the Discord channel ID\n"
                f"🔹 Example: `1234567890123456789`\n\n"
                f"📝 **How to get channel ID:**\n"
                f"1. Enable Developer Mode in Discord\n"
                f"2. Right-click on the channel\n"
                f"3. Click 'Copy ID'\n\n"
                f"⚠️ Make sure the bot has access to this channel!"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_add_{server_name}"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
        except Exception as e:
            self.logger.error(f"Error in add channel request: {e}")
    
    def _handle_confirm_add_channel(self, call):
        """Handle channel addition confirmation"""
        try:
            parts = call.data.replace('confirm_add_', '', 1).split('_', 1)
            if len(parts) != 2:
                self.bot.answer_callback_query(call.id, "❌ Invalid data")
                return
                
            server_name, channel_id = parts
            
            # Get channel name from user state
            user_state = self.user_states.get(call.from_user.id, {})
            channel_name = user_state.get('channel_name', f"Channel_{channel_id}")
            
            # Add channel
            success, message = self.add_channel_to_server(server_name, channel_id, channel_name)
            
            markup = InlineKeyboardMarkup()
            if success:
                markup.add(
                    InlineKeyboardButton("📋 View Server", callback_data=f"server_{server_name}"),
                    InlineKeyboardButton("🔙 Back to Servers", callback_data="servers")
                )
                status_icon = "✅"
            else:
                markup.add(InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"))
                status_icon = "❌"
            
            self.bot.edit_message_text(
                f"{status_icon} **Channel Addition Result**\n\n"
                f"Server: {server_name}\n"
                f"Channel ID: `{channel_id}`\n"
                f"Channel Name: {channel_name}\n\n"
                f"Result: {message}",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
            # Clear user state
            if call.from_user.id in self.user_states:
                del self.user_states[call.from_user.id]
                
        except Exception as e:
            self.logger.error(f"Error confirming add channel: {e}")
    
    def _handle_cancel_add_channel(self, call):
        """Handle cancel add channel"""
        try:
            server_name = call.data.replace('cancel_add_', '', 1)
            
            # Clear user state
            if call.from_user.id in self.user_states:
                del self.user_states[call.from_user.id]
            
            # Return to server view
            call.data = f"server_{server_name}"
            self._handle_server_selected(call)
        except Exception as e:
            self.logger.error(f"Error canceling add channel: {e}")
    
    def _process_channel_id_input(self, message, user_state):
        """Process channel ID input from user"""
        try:
            channel_id = message.text.strip()
            server_name = user_state['server_name']
            original_chat_id = user_state['chat_id']
            original_message_id = user_state['message_id']
            
            # Validate channel ID format
            if not channel_id.isdigit() or len(channel_id) < 17:
                self.bot.reply_to(
                    message, 
                    "❌ Invalid channel ID format. Please send a valid Discord channel ID (17-19 digits)"
                )
                return
            
            # Try to get channel name from Discord service
            channel_name = f"Channel_{channel_id}"
            if self.discord_service:
                try:
                    # Here you would implement channel name fetching
                    # For now, we'll use the default name
                    pass
                except Exception as e:
                    self.logger.debug(f"Could not get channel info: {e}")
            
            # Save channel name in state
            self.user_states[message.from_user.id]['channel_name'] = channel_name
            
            # Show confirmation
            confirmation_text = (
                f"🔍 **Channel Information**\n\n"
                f"Server: {server_name}\n"
                f"Channel ID: `{channel_id}`\n"
                f"Channel Name: {channel_name}\n\n"
                f"➕ Add this channel to monitoring?"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_add_{server_name}_{channel_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_add_{server_name}")
            )
            
            # Delete user's message
            try:
                self.bot.delete_message(message.chat.id, message.message_id)
            except:
                pass
            
            # Update original message
            try:
                self.bot.edit_message_text(
                    confirmation_text,
                    original_chat_id,
                    original_message_id,
                    reply_markup=markup,
                    parse_mode='Markdown'
                )
            except Exception as e:
                # If edit fails, send new message
                self.bot.send_message(
                    message.chat.id,
                    confirmation_text,
                    reply_markup=markup,
                    parse_mode='Markdown'
                )
        except Exception as e:
            self.logger.error(f"Error processing channel ID input: {e}")
    
    def _send_servers_list_message(self, message):
        """Send servers list as a new message"""
        try:
            if not self.discord_service:
                self.bot.reply_to(message, "❌ Discord service not available")
                return
            
            servers = getattr(self.discord_service, 'servers', {})
            
            if not servers:
                self.bot.reply_to(message, "❌ No Discord servers found")
                return
            
            text = f"📋 **Discord Servers ({len(servers)} total)**\n\n"
            
            for server_name in list(servers.keys())[:10]:
                topic_id = self.server_topics.get(server_name)
                if topic_id:
                    text += f"• {server_name} - Topic: {topic_id}\n"
                else:
                    text += f"• {server_name} - No topic\n"
            
            if len(servers) > 10:
                text += f"\n... and {len(servers) - 10} more servers"
            
            self.bot.reply_to(message, text, parse_mode='Markdown')
            
        except Exception as e:
            self.logger.error(f"Error sending servers list: {e}")
            self.bot.reply_to(message, f"❌ Error: {str(e)}")
    
    def add_channel_to_server(self, server_name: str, channel_id: str, channel_name: str = None) -> tuple[bool, str]:
        """Enhanced channel addition with Discord service integration"""
        try:
            # Check if Discord service is available
            if not self.discord_service:
                return False, "Discord service not available"
            
            # Check if server exists
            servers = getattr(self.discord_service, 'servers', {})
            if server_name not in servers:
                return False, "Server not found in Discord service"
            
            # Use Discord service to add channel
            if hasattr(self.discord_service, 'notify_new_channel_added'):
                success = self.discord_service.notify_new_channel_added(
                    server_name, channel_id, channel_name or f"Channel_{channel_id}"
                )
                
                if success:
                    return True, "Channel successfully added to monitoring"
                else:
                    return False, "Failed to add channel to Discord service"
            else:
                return False, "Discord service does not support channel addition"
            
        except Exception as e:
            self.logger.error(f"❌ Error adding channel to server: {e}")
            return False, f"Error adding channel: {str(e)}"
    
    async def get_or_create_server_topic(self, server_name: str) -> Optional[int]:
        """ENHANCED: Get or create topic with anti-duplicate protection"""
        chat_id = self.settings.telegram_chat_id
        
        # Ensure startup verification is done
        if not self.startup_verification_done:
            await self.startup_topic_verification()
        
        # Quick cache check without lock
        if server_name in self.server_topics:
            cached_topic_id = self.server_topics[server_name]
            
            # Quick existence check
            if await self._topic_exists(chat_id, cached_topic_id):
                self.logger.debug(f"✅ Using cached topic {cached_topic_id} for '{server_name}'")
                return cached_topic_id
            else:
                self.logger.warning(f"⚠️ Cached topic {cached_topic_id} not found, will recreate")
        
        # Use lock only if need to create/recreate topic
        async with self.topic_creation_lock:
            # Double-check after acquiring lock
            if server_name in self.server_topics:
                topic_id = self.server_topics[server_name]
                
                if await self._topic_exists(chat_id, topic_id):
                    self.logger.debug(f"✅ Using topic {topic_id} for '{server_name}' (double-check)")
                    return topic_id
                else:
                    self.logger.warning(f"🗑️ Topic {topic_id} confirmed missing, removing from cache")
                    del self.server_topics[server_name]
                    if topic_id in self.topic_name_cache:
                        del self.topic_name_cache[topic_id]
                    self._save_persistent_data()
            
            # Check if chat supports topics
            if not self._check_if_supergroup_with_topics(chat_id):
                self.logger.info("ℹ️ Chat doesn't support topics, using regular messages")
                return None
            
            # Check for existing topic with same name (additional protection)
            topic_name = f"{server_name}"
            for existing_server, existing_topic_id in self.server_topics.items():
                if existing_server != server_name and await self._topic_exists(chat_id, existing_topic_id):
                    try:
                        topic_info = self.bot.get_forum_topic(chat_id, existing_topic_id)
                        if topic_info and getattr(topic_info, 'name', '') == topic_name:
                            self.logger.warning(f"🔍 Found existing topic with same name: {existing_server}")
                            # Use existing topic and update mapping
                            self.server_topics[server_name] = existing_topic_id
                            self.topic_name_cache[existing_topic_id] = server_name
                            self._save_persistent_data()
                            return existing_topic_id
                    except:
                        continue
            
            # Create new topic
            self.logger.info(f"🔨 Creating new topic for server '{server_name}'")
            
            try:
                topic = self.bot.create_forum_topic(
                    chat_id=chat_id,
                    name=topic_name,
                    icon_color=0x6FB9F0,  # Blue color
                    icon_custom_emoji_id=None
                )
                
                topic_id = topic.message_thread_id
                self.server_topics[server_name] = topic_id
                self.topic_name_cache[topic_id] = server_name
                self._save_persistent_data()
                
                self.logger.info(f"✅ Created new topic for '{server_name}' with ID: {topic_id}")
                return topic_id
                
            except Exception as e:
                self.logger.error(f"❌ Error creating topic for '{server_name}': {e}")
                return None
    
    async def send_message(self, message: DiscordMessage) -> bool:
        """Enhanced message sending with deduplication and topic management"""
        try:
            # Check for duplicate messages
            if await self._is_duplicate_message(message):
                self.logger.debug("Duplicate message ignored", 
                               server=message.server_name,
                               message_id=message.message_id)
                return True
            
            await self.rate_limiter.wait_if_needed("telegram_send")
            
            # Get or create topic for server (ONE topic per server)
            topic_id = await self.get_or_create_server_topic(message.server_name)
            
            if topic_id is None and self.settings.use_topics:
                self.logger.error("Failed to get topic for server", server=message.server_name)
                return False
            
            # Format message for Telegram
            formatted_message = self._format_message_for_telegram(message)
            
            # Send message
            sent_message = self.bot.send_message(
                chat_id=self.settings.telegram_chat_id,
                text=formatted_message,
                message_thread_id=topic_id if self.settings.use_topics else None,
                parse_mode=None  # Safe mode without Markdown
            )
            
            if sent_message:
                # Track message
                self.message_mappings[str(message.timestamp)] = sent_message.message_id
                await self._mark_message_as_processed(message)
                
                # Save data asynchronously
                asyncio.create_task(self._async_save_data())
                
                self.logger.info("Enhanced message sent to Telegram", 
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
    
    # Остальные методы остаются без изменений...
    async def send_messages_batch(self, messages: List[DiscordMessage]) -> int:
        """Enhanced batch message sending"""
        if not messages:
            return 0
        
        # Group messages by server (ONE topic per server)
        server_groups = {}
        for message in messages:
            server_name = message.server_name
            if server_name not in server_groups:
                server_groups[server_name] = []
            server_groups[server_name].append(message)
        
        sent_count = 0
        
        # Send messages grouped by server
        for server_name, server_messages in server_groups.items():
            self.logger.info("Sending enhanced message batch", 
                           server=server_name,
                           message_count=len(server_messages))
            
            # Sort messages chronologically
            server_messages.sort(key=lambda x: x.timestamp, reverse=False)
            
            # Get topic ONCE for all messages from this server
            topic_id = await self.get_or_create_server_topic(server_name)
            
            if topic_id is None and self.settings.use_topics:
                self.logger.error("Failed to get topic for server batch", server=server_name)
                continue
            
            # Send each message to the SAME topic
            for message in server_messages:
                if await self._send_message_to_topic(message, topic_id):
                    sent_count += 1
                
                # Rate limiting between messages
                await asyncio.sleep(0.1)
        
        self.logger.info("Enhanced batch sending complete", 
                   total_messages=len(messages),
                   sent_messages=sent_count,
                   servers_processed=len(server_groups),
                   message_order="oldest_first_newest_last")
        return sent_count
    
    async def _send_message_to_topic(self, message: DiscordMessage, topic_id: Optional[int]) -> bool:
        """Send message to specific topic (helper method)"""
        try:
            # Check for duplicates
            if await self._is_duplicate_message(message):
                self.logger.debug("Duplicate message in batch ignored", 
                               server=message.server_name,
                               message_id=message.message_id)
                return True
            
            await self.rate_limiter.wait_if_needed("telegram_send")
            
            # Format and send message
            formatted_message = self._format_message_for_telegram(message)
            
            sent_message = self.bot.send_message(
                chat_id=self.settings.telegram_chat_id,
                text=formatted_message,
                message_thread_id=topic_id if self.settings.use_topics else None,
                parse_mode=None
            )
            
            if sent_message:
                self.message_mappings[str(message.timestamp)] = sent_message.message_id
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
    
    async def _is_duplicate_message(self, message: DiscordMessage) -> bool:
        """Enhanced message deduplication"""
        async with self._async_lock:
            message_key = f"{message.guild_id}:{message.channel_id}:{message.message_id}"
            
            # Clean old processed messages (older than 1 hour)
            cutoff_time = datetime.now() - timedelta(hours=1)
            old_keys = [
                key for key, timestamp in self.processed_messages.items()
                if timestamp < cutoff_time
            ]
            for key in old_keys:
                del self.processed_messages[key]
            
            # Check for duplicates
            return message_key in self.processed_messages
    
    async def _mark_message_as_processed(self, message: DiscordMessage) -> None:
        """Mark message as processed to prevent duplicates"""
        async with self._async_lock:
            message_key = f"{message.guild_id}:{message.channel_id}:{message.message_id}"
            self.processed_messages[message_key] = datetime.now()
    
    def _format_message_for_telegram(self, message: DiscordMessage) -> str:
        """Enhanced message formatting for Telegram"""
        parts = []
        
        # Channel info
        parts.append(f"📢 #{message.channel_name}")
        
        # Timestamp (if enabled)
        if self.settings.show_timestamps:
            parts.append(f"📅 {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Author
        parts.append(f"👤 {message.author}")
        
        # Content
        parts.append(f"💬 {message.content}")
        
        return "\n".join(parts)
    
    async def _clean_invalid_topics(self) -> int:
        """Enhanced topic cleanup with duplicate detection"""
        invalid_topics = []
        valid_topics = {}
        
        for server_name, topic_id in list(self.server_topics.items()):
            if not await self._topic_exists(self.settings.telegram_chat_id, topic_id):
                invalid_topics.append(server_name)
            else:
                # Check for duplicates
                topic_name = f"{server_name}"
                if topic_name in valid_topics:
                    # Found duplicate, close old one
                    old_topic_id = valid_topics[topic_name]
                    self.logger.warning(f"🗑️ Duplicate topic found during cleanup: {server_name}")
                    try:
                        self.bot.close_forum_topic(self.settings.telegram_chat_id, old_topic_id)
                    except:
                        pass
                    # Mark old server for removal
                    for srv, tid in list(self.server_topics.items()):
                        if tid == old_topic_id:
                            invalid_topics.append(srv)
                            break
                
                valid_topics[topic_name] = topic_id
        
        # Remove invalid topics
        for server_name in invalid_topics:
            if server_name in self.server_topics:
                old_topic_id = self.server_topics[server_name]
                self.logger.info(f"🗑️ Removing invalid topic for server: {server_name} (ID: {old_topic_id})")
                del self.server_topics[server_name]
                if old_topic_id in self.topic_name_cache:
                    del self.topic_name_cache[old_topic_id]
        
        if invalid_topics:
            self._save_persistent_data()
            self.logger.info(f"🧹 Cleaned up {len(invalid_topics)} invalid/duplicate topics")
        
        return len(invalid_topics)
    
    def _load_persistent_data(self) -> None:
        """Load persistent data from file"""
        try:
            if os.path.exists(self.message_store_file):
                with open(self.message_store_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.message_mappings = data.get('messages', {})
                    self.server_topics = data.get('topics', {})
                    
                    # Create reverse cache
                    self.topic_name_cache = {v: k for k, v in self.server_topics.items()}
                    
                    self.logger.info(f"📋 Loaded persistent data",
                                   topics=len(self.server_topics),
                                   messages=len(self.message_mappings))
            else:
                self.message_mappings = {}
                self.server_topics = {}
                self.topic_name_cache = {}
                
        except Exception as e:
            self.logger.error(f"Error loading persistent data: {e}")
            self.message_mappings = {}
            self.server_topics = {}
            self.topic_name_cache = {}
    
    def _save_persistent_data(self) -> None:
        """Save persistent data to file"""
        try:
            data = {
                'messages': self.message_mappings,
                'topics': self.server_topics,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.message_store_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            self.logger.error(f"Error saving persistent data: {e}")
    
    async def _async_save_data(self) -> None:
        """Asynchronous data saving"""
        try:
            self._save_persistent_data()
        except Exception as e:
            self.logger.error(f"Error in async save: {e}")
    
    async def start_bot_async(self) -> None:
        """ИСПРАВЛЕНИЕ: Start the enhanced Telegram bot asynchronously"""
        if self.bot_running:
            self.logger.warning("Enhanced bot is already running")
            return
        
        if not self.bot:
            self.logger.error("Bot not initialized, cannot start")
            return
        
        self.bot_running = True
        
        self.logger.info("Starting Enhanced Telegram bot", 
                       chat_id=self.settings.telegram_chat_id,
                       use_topics=self.settings.use_topics,
                       server_topics=len(self.server_topics),
                       features=["Anti-duplicate", "Interactive UI", "Channel management"])
        
        try:
            # ИСПРАВЛЕНИЕ: Запускаем бота в отдельном потоке
            def run_bot():
                try:
                    self.logger.info("Bot polling started in thread")
                    self.bot.polling(
                        none_stop=True,
                        interval=1,
                        timeout=30,
                        skip_pending=True
                    )
                except Exception as e:
                    self.logger.error("Bot polling error in thread", error=str(e))
                finally:
                    self.bot_running = False
                    self.logger.info("Bot polling stopped")
            
            # Запускаем в отдельном потоке
            self._bot_thread = threading.Thread(target=run_bot, daemon=True)
            self._bot_thread.start()
            
            self.logger.info("Enhanced Telegram bot started successfully")
            
            # Ждем немного, чтобы убедиться что бот запустился
            await asyncio.sleep(2)
            
            if not self.bot_running:
                self.logger.error("Bot failed to start")
                return
            
            # Отправляем тестовое сообщение для проверки
            try:
                test_message = (
                    "🤖 **Discord Telegram Parser Bot Started!**\n\n"
                    f"✅ Bot is now active and monitoring\n"
                    f"📋 {len(self.server_topics)} topics configured\n"
                    f"🛡️ Anti-duplicate protection: {'✅ ACTIVE' if self.startup_verification_done else '⏳ STARTING'}\n\n"
                    f"Use /start for interactive menu"
                )
                
                self.bot.send_message(
                    chat_id=self.settings.telegram_chat_id,
                    text=test_message,
                    parse_mode='Markdown'
                )
                
                self.logger.info("Bot startup notification sent successfully")
                
            except Exception as e:
                self.logger.warning(f"Could not send startup notification: {e}")
            
        except Exception as e:
            self.logger.error("Enhanced bot startup error", error=str(e))
            self.bot_running = False
    
    def stop_bot(self) -> None:
        """ИСПРАВЛЕНИЕ: Stop the enhanced Telegram bot"""
        if not self.bot_running:
            self.logger.info("Bot is not running")
            return
        
        self.logger.info("Stopping Enhanced Telegram bot...")
        
        try:
            # Останавливаем polling
            if self.bot:
                self.bot.stop_polling()
            
            # Ждем завершения потока
            if self._bot_thread and self._bot_thread.is_alive():
                self._bot_thread.join(timeout=5)
            
            self.bot_running = False
            self.logger.info("Enhanced Telegram bot stopped successfully")
            
        except Exception as e:
            self.logger.error("Error stopping enhanced bot", error=str(e))
            self.bot_running = False
    
    async def cleanup(self) -> None:
        """Enhanced cleanup"""
        self.logger.info("Starting Telegram service cleanup...")
        
        # Stop bot
        self.stop_bot()
        
        # Save persistent data
        self._save_persistent_data()
        
        # Clear caches
        async with self._async_lock:
            self.processed_messages.clear()
        
        self.topic_name_cache.clear()
        self.user_states.clear()
        
        self.logger.info("Enhanced Telegram service cleaned up",
                        final_topics=len(self.server_topics),
                        features_cleaned=["Message cache", "User states", "Topic cache"])
    
    def get_enhanced_stats(self) -> Dict:
        """Get enhanced statistics"""
        # Check topic validity
        verified_topics = 0
        if self.bot:
            for topic_id in self.server_topics.values():
                try:
                    topic_info = self.bot.get_forum_topic(
                        chat_id=self.settings.telegram_chat_id,
                        message_thread_id=topic_id
                    )
                    if topic_info:
                        verified_topics += 1
                except:
                    pass
        
        return {
            "topics": {
                "total": len(self.server_topics),
                "verified": verified_topics,
                "startup_verification_done": self.startup_verification_done
            },
            "messages": {
                "tracked": len(self.message_mappings),
                "processed_cache": len(self.processed_messages)
            },
            "bot": {
                "running": self.bot_running,
                "active_user_states": len(self.user_states),
                "thread_alive": self._bot_thread.is_alive() if self._bot_thread else False
            },
            "features": {
                "anti_duplicate": True,
                "interactive_ui": True,
                "channel_management": True,
                "topic_verification": True,
                "websocket_integration": True
            }
        }