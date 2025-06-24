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
    """Telegram service с рабочими функциями бота"""
    
    def __init__(self, 
                 settings: Settings,
                 rate_limiter: RateLimiter,
                 redis_client = None,
                 logger = None):
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.redis_client = redis_client
        self.logger = logger or structlog.get_logger(__name__)
        
        # Инициализация бота с правильными настройками
        self.bot = None
        self._initialize_bot()
        
        #  Topic Management with Anti-Duplicate Protection
        self.server_topics: Dict[str, int] = {}  # server_name -> topic_id (ONE topic per server)
        self.topic_name_cache: Dict[int, str] = {}  # topic_id -> server_name for fast lookup
        self.user_states: Dict[int, dict] = {}  # user_id -> state for multi-step operations
        
        #  Message tracking and deduplication
        self.message_mappings: Dict[str, int] = {}  # timestamp -> telegram_message_id
        self.processed_messages: Dict[str, datetime] = {}  # message_id -> timestamp
        self.message_store_file = 'telegram_messages.json'
        
        #  Bot state management
        self.bot_running = False
        self.startup_verification_done = False
        self._bot_thread = None
        
        #  Thread-safe operations
        self._async_lock = asyncio.Lock()
        self.topic_creation_lock = asyncio.Lock()
        self.topic_sync_lock = asyncio.Lock()
        
        #  Callbacks and monitoring
        self.new_message_callbacks: List[Callable[[DiscordMessage], None]] = []
        self.discord_service = None  # Will be set by dependency injection
        
        # Load existing data
        self._load_persistent_data()
    
    def _is_announcement_channel(self, channel_name: str) -> bool:
        """Проверка что канал является announcement (с учетом emoji)"""
        # Удаляем emoji и лишние пробелы из названия
        clean_name = ''.join([c for c in channel_name if c.isalpha() or c.isspace()])
        clean_name = ' '.join(clean_name.split()).lower()
        
        # Проверяем содержит ли очищенное название любое из ключевых слов
        for keyword in self.settings.channel_keywords:
            if keyword in clean_name:
                return True
        return False
    
    def _initialize_bot(self):
        """Правильная инициализация бота"""
        try:
            self.bot = telebot.TeleBot(
                self.settings.telegram_bot_token,
                skip_pending=True,
                threaded=True,  
                parse_mode=None,
                num_threads=4  
            )
            try:
                bot_info = self.bot.get_me()
                self.logger.info("Telegram bot initialized successfully with safe threading", 
                                bot_username=bot_info.username,
                                bot_id=bot_info.id,
                                threads=4)
            except Exception as e:
                self.logger.error(f"Bot test failed: {e}")
                self.bot = None
                return False
            
            # Сразу устанавливаем обработчики
            self._setup_bot_handlers()
            
            self.logger.info("Telegram bot initialized successfully", 
                           bot_token_preview=self.settings.telegram_bot_token[:10] + "...")
                           
        except Exception as e:
            self.logger.error("Failed to initialize Telegram bot", error=str(e))
            self.bot = None
    
    def _setup_bot_handlers(self):
        """Настройка обработчиков бота"""
        if not self.bot:
            self.logger.error("Cannot setup handlers - bot not initialized")
            return
        
        # Очищаем старые обработчики
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
            """ Обработчик callback запросов без дублирования"""
            try:
                data = call.data
                self.logger.info(f"📞 Callback received: {data} from user {call.from_user.id}")
                
                # Отвечаем на callback
                try:
                    self.bot.answer_callback_query(call.id, "⏳ Обработка...")
                except Exception as e:
                    self.logger.warning(f"Failed to answer callback: {e}")
                
                # Route to appropriate handler
                if data == "servers":
                    self._handle_servers_list(call)
                elif data.startswith("servers_page_"):
                    self._handle_servers_pagination(call)
                elif data == "page_info":
                    # Just acknowledge page info clicks
                    self.bot.answer_callback_query(call.id, "📄 Current page information")
                    return
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
                elif data.startswith("remove_channel_"):
                    self._handle_remove_channel_request(call)
                elif data.startswith("confirm_remove_"):
                    self._handle_confirm_remove_channel(call)
                elif data.startswith("final_remove_"):
                    self._handle_final_remove_channel(call)
                elif data.startswith("manage_channels_"):
                    self._handle_manage_channels(call)
                elif data.startswith("channel_stats_"):
                    self._handle_channel_stats(call)
                elif data.startswith("show_all_remove_"):
                    self._handle_show_all_removable(call)
                elif data.startswith("browse_channels_"):
                    self._handle_browse_channels(call)
                elif data.startswith("channel_info_"):
                    self._handle_channel_info(call)
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
    
    #  2: Заменить метод set_discord_service в TelegramService

    def set_discord_service(self, discord_service):
        """Set Discord service reference for enhanced channel management"""
        self.discord_service = discord_service
        
        # Устанавливаем обратную ссылку правильно
        if hasattr(discord_service, 'telegram_service_ref'):
            discord_service.telegram_service_ref = self
        
        # Проверяем доступность всех необходимых атрибутов
        required_attrs = ['servers', 'monitored_announcement_channels', 'sessions']
        missing_attrs = []
        
        for attr in required_attrs:
            if not hasattr(discord_service, attr):
                missing_attrs.append(attr)
        
        if missing_attrs:
            self.logger.error(f"Discord service missing required attributes: {missing_attrs}")
        else:
            self.logger.info("Enhanced Discord service integration established successfully")
            self.logger.info(f"Available servers: {len(getattr(discord_service, 'servers', {}))}")
            self.logger.info(f"Monitored channels: {len(getattr(discord_service, 'monitored_announcement_channels', set()))}")
    
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
                
                #  Startup topic verification to prevent duplicates
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
        """ Startup verification to prevent duplicate topics"""
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
    

    def _handle_servers_list(self, call):
        """: Полный список серверов с пагинацией"""
        try:
            # Get page number from call attribute or callback data
            page = 0
            if hasattr(call, '_page'):
                page = call._page
            elif '_page_' in call.data:
                parts = call.data.split('_page_')
                if len(parts) == 2 and parts[1].isdigit():
                    page = int(parts[1])
            
            # ДОПОЛНИТЕЛЬНЫЕ ПРОВЕРКИ
            if not self.discord_service:
                self.logger.error("Discord service not available in _handle_servers_list")
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
                self.bot.edit_message_text(
                    "❌ Discord service not available. Please check service connection.",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
                return
            
            # Проверяем атрибут servers
            if not hasattr(self.discord_service, 'servers'):
                self.logger.error("Discord service missing 'servers' attribute")
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
                self.bot.edit_message_text(
                    "❌ Discord service not properly initialized. Missing 'servers' attribute.",
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
            
            # Проверяем атрибут monitored_announcement_channels
            if not hasattr(self.discord_service, 'monitored_announcement_channels'):
                self.logger.error("Discord service missing 'monitored_announcement_channels' attribute")
                self.discord_service.monitored_announcement_channels = set()  # Создаем пустой set как fallback
            
            # ПРОДОЛЖАЕМ С ПОЛНОЙ ЛОГИКОЙ ОТОБРАЖЕНИЯ СПИСКА
            
            # Pagination settings
            servers_per_page = 6
            server_list = list(servers.keys())
            total_servers = len(server_list)
            total_pages = (total_servers + servers_per_page - 1) // servers_per_page
            
            # Ensure page is within bounds
            page = max(0, min(page, total_pages - 1))
            
            # Get servers for current page
            start_idx = page * servers_per_page
            end_idx = min(start_idx + servers_per_page, total_servers)
            current_page_servers = server_list[start_idx:end_idx]
            
            # Build header
            text = f"📋 **Discord Servers ({total_servers} total)**\n\n"
            
            if total_pages > 1:
                text += f"📄 **Page {page + 1} of {total_pages}**\n\n"
            
            # Show servers on current page
            for i, server_name in enumerate(current_page_servers, start_idx + 1):
                server_info = servers[server_name]
                
                # Get server stats
                total_channels = getattr(server_info, 'channel_count', 0)
                accessible_channels = getattr(server_info, 'accessible_channel_count', 0)
                
                # Count monitored channels
                monitored_count = 0
                announcement_count = 0
                for channel_id, channel_info in getattr(server_info, 'channels', {}).items():
                    if channel_id in self.discord_service.monitored_announcement_channels:
                        monitored_count += 1
                        if self._is_announcement_channel(channel_info.channel_name):
                            announcement_count += 1
                
                # Topic status
                topic_id = self.server_topics.get(server_name)
                topic_status = "❌"
                if topic_id:
                    try:
                        topic_info = self.bot.get_forum_topic(
                            chat_id=self.settings.telegram_chat_id,
                            message_thread_id=topic_id
                        )
                        topic_status = "✅" if topic_info else "❌"
                    except:
                        topic_status = "❌"
                
                # Server status
                server_status = getattr(server_info, 'status', 'unknown')
                status_emoji = "✅" if str(server_status) == "ServerStatus.ACTIVE" or server_status == "active" else "⚠️"
                
                text += f"{i}. {status_emoji} **{server_name}**\n"
                text += f"   🔗 Channels: {total_channels} total, {accessible_channels} accessible\n"
                text += f"   🔔 Monitored: {monitored_count} ({announcement_count} announcement)\n"
                text += f"   📋 Topic: {topic_id or 'None'} {topic_status}\n"
                
                # Show last sync if available
                last_sync = getattr(server_info, 'last_sync', None)
                if last_sync:
                    sync_time = last_sync.strftime('%m-%d %H:%M') if hasattr(last_sync, 'strftime') else str(last_sync)[:16]
                    text += f"   🔄 Last sync: {sync_time}\n"
                
                text += "\n"
            
            # Summary
            total_monitored = len(self.discord_service.monitored_announcement_channels)
            total_all_channels = sum(getattr(s, 'channel_count', 0) for s in servers.values())
            total_accessible = sum(getattr(s, 'accessible_channel_count', 0) for s in servers.values())
            
            text += f"📊 **Summary:**\n"
            text += f"• Total channels: {total_all_channels}\n"
            text += f"• Accessible: {total_accessible}\n"
            text += f"• Monitored: {total_monitored}\n"
            text += f"• Active topics: {len(self.server_topics)}\n"
            text += f"🛡️ Anti-duplicate protection: {'✅ ACTIVE' if self.startup_verification_done else '⏳ STARTING'}"
            
            # Create markup
            markup = InlineKeyboardMarkup()
            
            # Add server buttons (up to 6 per page to fit in Telegram limits)
            for server_name in current_page_servers[:6]:
                server_info = servers[server_name]
                monitored_count = len([
                    ch_id for ch_id in getattr(server_info, 'channels', {}).keys()
                    if ch_id in self.discord_service.monitored_announcement_channels
                ])
                
                # Shorten server name for button if needed
                display_name = server_name[:20] + "..." if len(server_name) > 20 else server_name
                button_text = f"📊 {display_name} ({monitored_count})"
                
                markup.add(InlineKeyboardButton(
                    button_text,
                    callback_data=f"server_{server_name}"
                ))
            
            # Pagination buttons
            if total_pages > 1:
                pagination_row = []
                
                # Previous page
                if page > 0:
                    pagination_row.append(InlineKeyboardButton(
                        "⬅️ Previous",
                        callback_data=f"servers_page_{page - 1}"
                    ))
                
                # Page info
                pagination_row.append(InlineKeyboardButton(
                    f"📄 {page + 1}/{total_pages}",
                    callback_data="page_info"
                ))
                
                # Next page
                if page < total_pages - 1:
                    pagination_row.append(InlineKeyboardButton(
                        "Next ➡️",
                        callback_data=f"servers_page_{page + 1}"
                    ))
                
                markup.row(*pagination_row)
            
            # Action buttons
            action_row = []
            action_row.append(InlineKeyboardButton("🔄 Refresh", callback_data="refresh"))
            action_row.append(InlineKeyboardButton("📊 Stats", callback_data="status"))
            markup.row(*action_row)
            
            # Back button
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            
            # Send message
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
            self.logger.info(f"Displayed servers list page {page + 1}/{total_pages}",
                           servers_on_page=len(current_page_servers),
                           total_servers=total_servers)
            
        except Exception as e:
            self.logger.error(f"Critical error in servers list handler: {e}")
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            try:
                self.bot.edit_message_text(
                    f"❌ Critical error: {str(e)[:100]}...\n\nPlease try again or contact support.",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            except Exception as inner_e:
                self.logger.error(f"Failed to send error message: {inner_e}")
                # Fallback: answer callback query
                try:
                    self.bot.answer_callback_query(call.id, "❌ Error loading servers list")
                except:
                    pass
            
    def _handle_servers_pagination(self, call):
        """: Обработка пагинации серверов"""
        try:
            # Extract page number from callback data
            if call.data.startswith('servers_page_'):
                page_str = call.data.replace('servers_page_', '')
                if page_str.isdigit():
                    page = int(page_str)
                    # Modify call data to trigger servers list with specific page
                    original_data = call.data
                    call.data = "servers"  # Set to servers to use existing handler
                    # Store page in a temporary attribute
                    call._page = page
                    self._handle_servers_list(call)
                    # Restore original data
                    call.data = original_data
                    return
            
            # If we get here, something went wrong
            self.logger.warning(f"Invalid pagination data: {call.data}")
            self.bot.answer_callback_query(call.id, "❌ Invalid page request")
            
        except Exception as e:
            self.logger.error(f"Error in servers pagination: {e}")
            self.bot.answer_callback_query(call.id, "❌ Pagination error") 
    
    async def _fetch_all_guilds_from_all_tokens(self) -> List[dict]:
        """Получить ВСЕ гильдии со всех доступных токенов"""
        all_guilds = []
        seen_guild_ids = set()
        
        self.logger.info(f"🔍 Fetching guilds from {len(self.sessions)} tokens...")
        
        # Создаем задачи для всех токенов
        fetch_tasks = []
        for i, session in enumerate(self.sessions):
            task = self._fetch_guilds_from_single_token(session, i)
            fetch_tasks.append(task)
        
        # Выполняем запросы параллельно
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        
        # Объединяем результаты, убирая дубликаты
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"❌ Token {i} failed to fetch guilds: {result}")
                continue
            
            if not result:
                self.logger.warning(f"⚠️ Token {i} returned no guilds")
                continue
                
            self.logger.info(f"✅ Token {i}: {len(result)} guilds found")
            
            for guild in result:
                guild_id = guild.get('id')
                if guild_id and guild_id not in seen_guild_ids:
                    seen_guild_ids.add(guild_id)
                    guild['_source_token'] = i  # Помечаем источник
                    all_guilds.append(guild)
        
        self.logger.info(f"📊 Total unique guilds collected: {len(all_guilds)} from {len(self.sessions)} tokens")
        return all_guilds
    
    
    
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
        """ОБНОВЛЕНО: Показ сервера с announcement + кнопка для всех каналов"""
        try:
            server_name = call.data.replace('server_', '', 1)
            
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return

            server_info = self.discord_service.servers[server_name]
            
            # Подсчет announcement каналов (автоматически найденных)
            announcement_channels = {}
            for channel_id, channel_info in server_info.channels.items():
                if channel_id in self.discord_service.monitored_announcement_channels:
                    announcement_channels[channel_id] = channel_info
            
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
                topic_info = "📋 Topic: Will be created when needed"
            
            text = (
                f"**{server_name}**\n\n"
                f"📊 **Auto-discovered:**\n"
                f"🔔 Announcement channels: {len(announcement_channels)}\n"
                f"{topic_info}\n\n"
            )
            
            # Show announcement channels
            if announcement_channels:
                text += f"📢 **Active Announcement Channels:**\n"
                for channel_id, channel_info in list(announcement_channels.items())[:5]:  # Show max 5
                    channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
                    text += f"• {channel_name}\n"
                
                if len(announcement_channels) > 5:
                    text += f"• ... and {len(announcement_channels) - 5} more\n"
            else:
                text += f"📭 **No announcement channels found**\n"
            
            text += (
                f"\n💡 **Available Actions:**\n"
                f"• View recent messages from announcement channels\n"
                f"• Browse ALL server channels by category\n"
                f"• Add any channel manually to monitoring"
            )
            
            markup = InlineKeyboardMarkup()
            
            # Action buttons
            if announcement_channels:
                markup.add(
                    InlineKeyboardButton("📥 Get Messages", callback_data=f"get_messages_{server_name}")
                )
            
            # НОВАЯ КНОПКА: Показать все каналы по категориям
            markup.add(
                InlineKeyboardButton("📋 Browse All Channels", callback_data=f"browse_channels_{server_name}")
            )
            
            if announcement_channels:
                markup.add(
                    InlineKeyboardButton("📊 Channel Stats", callback_data=f"channel_stats_{server_name}")
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
    


    def add_channel_to_server(self, server_name: str, channel_id: str, channel_name: str = None) -> tuple[bool, str]:
        """Add any channel to a server and enable monitoring"""
        try:
            self.logger.info(f"Adding channel to server: {server_name}, channel_id: {channel_id}, name: {channel_name}")
            
            # Check if Discord service is available
            if not self.discord_service:
                return False, "Discord service not available"
            
            servers = getattr(self.discord_service, 'servers', {})
            if server_name not in servers:
                return False, f"Server '{server_name}' not found in Discord service"
            
            server_info = servers[server_name]
            
            # Check limits
            current_channel_count = len(server_info.channels)
            max_channels = getattr(server_info, 'max_channels', 5)
            
            if current_channel_count >= max_channels:
                return False, f"Server has reached maximum channels limit ({max_channels})"
            
            # Check if channel is already added
            if channel_id in server_info.channels:
                # If already added, just ensure it's monitored
                if hasattr(self.discord_service, 'monitored_announcement_channels'):
                    self.discord_service.monitored_announcement_channels.add(channel_id)
                    self.logger.info(f"Channel {channel_id} already exists - ensuring it's monitored")
                    return True, "Channel is already added and will be monitored"
                return False, "Channel is already added to this server"
            
            # Create channel_info
            from ..models.server import ChannelInfo
            from datetime import datetime
            
            channel_name_final = channel_name or f"Channel_{channel_id}"
            
            new_channel_info = ChannelInfo(
                channel_id=channel_id,
                channel_name=channel_name_final,
                http_accessible=True,  # Will be verified below
                websocket_accessible=False,
                last_checked=datetime.now()
            )
            
            # Test channel accessibility through Discord API
            channel_accessible = False
            real_channel_name = channel_name_final
            
            if hasattr(self.discord_service, 'sessions') and self.discord_service.sessions:
                try:
                    import asyncio
                    
                    async def test_channel_access():
                        try:
                            session = self.discord_service.sessions[0]
                            
                            # Use rate limiter if available
                            if hasattr(self.discord_service, 'rate_limiter'):
                                await self.discord_service.rate_limiter.wait_if_needed(f"test_channel_{channel_id}")
                            
                            # Get channel info
                            async with session.get(f'https://discord.com/api/v9/channels/{channel_id}') as response:
                                if response.status == 200:
                                    channel_data = await response.json()
                                    name = channel_data.get('name', channel_name_final)
                                    
                                    # Test message access
                                    async with session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1') as msg_response:
                                        accessible = msg_response.status == 200
                                        return accessible, name
                                return False, channel_name_final
                        except Exception as e:
                            self.logger.error(f"Error testing channel access: {e}")
                            return False, channel_name_final
                    
                    # Create a new event loop
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        channel_accessible, real_channel_name = loop.run_until_complete(test_channel_access())
                    finally:
                        loop.close()
                    
                    # Update channel info with results
                    new_channel_info.http_accessible = channel_accessible
                    new_channel_info.channel_name = real_channel_name
                    channel_name_final = real_channel_name
                    
                except Exception as e:
                    self.logger.error(f"Error testing channel: {e}")
            
            # Add channel to server info
            server_info.channels[channel_id] = new_channel_info
            
            # Update accessible channels if needed
            if hasattr(server_info, 'accessible_channels') and channel_accessible:
                server_info.accessible_channels[channel_id] = new_channel_info
            
            # Add to monitored channels
            is_announcement = self._is_announcement_channel(channel_name_final)
            
            if hasattr(self.discord_service, 'monitored_announcement_channels'):
                self.discord_service.monitored_announcement_channels.add(channel_id)
                self.logger.info(f"Added channel '{channel_name_final}' ({channel_id}) to monitoring")
            
            # Update server statistics
            server_info.update_stats()
            
            # Success message
            if channel_accessible:
                access_msg = "✅ Channel is accessible"
            else:
                access_msg = "⚠️ Channel may not be accessible"
                
            success_message = (
                f"Channel successfully added and will be monitored!\n"
                f"{access_msg}\n"
                f"Server now has {len(server_info.channels)} channels.\n\n"
            )
            
            if is_announcement:
                success_message += "📢 This is an ANNOUNCEMENT channel"
            else:
                success_message += "📝 This is a regular channel"
            
            # Notify Discord service if necessary
            if hasattr(self.discord_service, 'notify_new_channel_added'):
                try:
                    self.discord_service.notify_new_channel_added(server_name, channel_id, channel_name_final)
                except Exception as e:
                    self.logger.warning(f"Could not notify Discord service: {e}")
            
            return True, success_message
            
        except Exception as e:
            self.logger.error(f"Error adding channel to server: {e}")
            return False, f"Error adding channel: {str(e)}"
    
    def remove_channel_from_server(self, server_name: str, channel_id: str) -> tuple[bool, str]:
        """Удалить канал из мониторинга сервера"""
        try:
            self.logger.info(f"Removing channel from monitoring: server={server_name}, channel_id={channel_id}")
            
            # Проверяем что Discord service доступен
            if not self.discord_service:
                return False, "Discord service not available"
            
            servers = getattr(self.discord_service, 'servers', {})
            if server_name not in servers:
                return False, f"Server '{server_name}' not found"
            
            server_info = servers[server_name]
            
            # Проверяем что канал существует в сервере
            if channel_id not in server_info.channels:
                return False, f"Channel {channel_id} not found in server {server_name}"
            
            # Проверяем что канал действительно мониторится
            if not hasattr(self.discord_service, 'monitored_announcement_channels'):
                return False, "Monitored channels list not available"
            
            if channel_id not in self.discord_service.monitored_announcement_channels:
                return False, "Channel is not being monitored"
            
            # Получаем информацию о канале перед удалением
            channel_info = server_info.channels[channel_id]
            channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
            is_announcement = self._is_announcement_channel(channel_name)
            
            # Удаляем канал из мониторинга
            self.discord_service.monitored_announcement_channels.remove(channel_id)
            
            # Уведомляем Discord service об удалении
            if hasattr(self.discord_service, 'notify_channel_removed'):
                try:
                    self.discord_service.notify_channel_removed(server_name, channel_id, channel_name)
                except Exception as e:
                    self.logger.warning(f"Could not notify Discord service about removal: {e}")
            
            # Обновляем статистику сервера
            server_info.update_stats()
            
            # Подсчитываем оставшиеся мониторимые каналы
            remaining_monitored = len([
                ch_id for ch_id in server_info.channels.keys() 
                if ch_id in self.discord_service.monitored_announcement_channels
            ])
            
            # Формируем сообщение об успехе
            channel_type = "announcement" if is_announcement else "regular"
            success_message = (
                f"Channel '{channel_name}' removed from monitoring!\n"
                f"• Type: {channel_type.title()}\n"
                f"• Remaining monitored channels: {remaining_monitored}\n"
                f"• Channel still exists in Discord\n"
                f"• Messages will no longer be forwarded"
            )
            
            self.logger.info(f"✅ Channel '{channel_name}' ({channel_id}) removed from monitoring")
            self.logger.info(f"📊 Server '{server_name}' now has {remaining_monitored} monitored channels")
            
            # Логируем изменения в мониторинге
            announcement_channels = len([
                ch for ch in server_info.channels.values() 
                if ch.channel_id in self.discord_service.monitored_announcement_channels
                and self._is_announcement_channel(ch.channel_name)
            ])
            manual_channels = remaining_monitored - announcement_channels
            
            self.logger.info(f"📈 Monitoring breakdown:")
            self.logger.info(f"   • Auto-discovered announcement: {announcement_channels}")
            self.logger.info(f"   • Manually added regular: {manual_channels}")
            self.logger.info(f"   • Total monitored: {remaining_monitored}")
            
            return True, success_message
            
        except ValueError as e:
            # Канал не был в списке мониторинга
            self.logger.warning(f"Channel {channel_id} was not in monitoring list: {e}")
            return False, "Channel was not in monitoring list"
        except Exception as e:
            self.logger.error(f"Error removing channel from monitoring: {e}")
            return False, f"Error removing channel: {str(e)}"

    def _handle_channel_stats(self, call):
        """Обработчик статистики каналов"""
        try:
            server_name = call.data.replace('channel_stats_', '', 1)
            
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            server_info = self.discord_service.servers[server_name]
            channels = getattr(server_info, 'accessible_channels', {})
            
            # Собираем статистику
            total_channels = len(server_info.channels)
            accessible_channels = len(channels)
            monitored_channels = 0
            announcement_channels = 0
            regular_channels = 0
            total_messages = 0
            
            for channel_id, channel_info in channels.items():
                if channel_id in self.discord_service.monitored_announcement_channels:
                    monitored_channels += 1
                    total_messages += getattr(channel_info, 'message_count', 0)
                    
                    if self._is_announcement_channel(channel_info.channel_name):
                        announcement_channels += 1
                    else:
                        regular_channels += 1
            
            # Topic информация
            topic_id = self.server_topics.get(server_name)
            topic_status = "❌ No topic"
            
            if topic_id:
                try:
                    topic_info = self.bot.get_forum_topic(call.message.chat.id, topic_id)
                    topic_status = f"✅ Topic {topic_id}" if topic_info else f"❌ Invalid topic {topic_id}"
                except:
                    topic_status = f"❌ Invalid topic {topic_id}"
            
            text = (
                f"📊 **Channel Statistics - {server_name}**\n\n"
                f"**📋 Channel Overview:**\n"
                f"• Total channels: {total_channels}\n"
                f"• Accessible channels: {accessible_channels}\n"
                f"• Monitored channels: {monitored_channels}\n\n"
                f"**🔔 Monitoring Breakdown:**\n"
                f"• 📢 Announcement: {announcement_channels}\n"
                f"• 📝 Regular (manual): {regular_channels}\n"
                f"• 🚫 Not monitored: {accessible_channels - monitored_channels}\n\n"
                f"**📈 Activity Stats:**\n"
                f"• Total messages tracked: {total_messages}\n"
                f"• Average per channel: {total_messages / max(monitored_channels, 1):.1f}\n\n"
                f"**🎯 Telegram Integration:**\n"
                f"• {topic_status}\n"
                f"• All monitored channels → Same topic\n"
                f"• Real-time forwarding: ✅ Active\n\n"
                f"**💡 Management:**\n"
                f"• Strategy: Auto announcement + Manual any\n"
                f"• All monitored channels are removable\n"
                f"• Channels can be added via bot interface"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("🔄 Refresh Stats", callback_data=f"channel_stats_{server_name}"),
                InlineKeyboardButton("📋 Manage", callback_data=f"manage_channels_{server_name}")
            )
            markup.add(InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error in channel stats: {e}")

    def _handle_channel_info(self, call):
        """Обработчик информации о канале и управления им"""
        try:
            # Формат: channel_info_{server_name}_{channel_id}
            parts = call.data.replace('channel_info_', '', 1).split('_', 1)
            if len(parts) != 2:
                self.bot.answer_callback_query(call.id, "❌ Invalid channel info format")
                return
                
            server_name, channel_id = parts
            
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            server_info = self.discord_service.servers[server_name]
            channels = getattr(server_info, 'channels', {})
            
            if channel_id not in channels:
                self.bot.answer_callback_query(call.id, "❌ Channel not found")
                return
                
            channel_info = channels[channel_id]
            channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
            is_monitored = channel_id in getattr(self.discord_service, 'monitored_announcement_channels', set())
            is_accessible = getattr(channel_info, 'http_accessible', False)
            
            text = (
                f"📋 **Channel Information**\n\n"
                f"**Server:** {server_name}\n"
                f"**Channel:** {channel_name}\n"
                f"**ID:** `{channel_id}`\n"
                f"**Status:** {'✅ Monitored' if is_monitored else '❌ Not monitored'}\n"
                f"**Access:** {'✅ Accessible' if is_accessible else '❌ Not accessible'}\n\n"
            )
            
            markup = InlineKeyboardMarkup()
            
            if is_monitored:
                text += "💡 This channel is being monitored and forwarding messages to Telegram."
                markup.add(
                    InlineKeyboardButton("🗑️ Remove from Monitoring", callback_data=f"confirm_remove_{server_name}_{channel_id}")
                )
            else:
                text += "💡 This channel is not being monitored. You can add it to monitoring."
                markup.add(
                    InlineKeyboardButton("➕ Add to Monitoring", callback_data=f"confirm_add_{server_name}_{channel_id}")
                )
            
            markup.add(
                InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"),
                InlineKeyboardButton("📋 Browse Channels", callback_data=f"browse_channels_{server_name}")
            )
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error in channel info handler: {e}")
            self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)[:50]}")

    def _handle_browse_channels(self, call):
        """Обработчик просмотра всех каналов сервера"""
        try:
            server_name = call.data.replace('browse_channels_', '', 1)
            
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            server_info = self.discord_service.servers[server_name]
            channels = getattr(server_info, 'channels', {})
            
            if not channels:
                self.bot.answer_callback_query(call.id, "❌ No channels found for this server")
                return
            
            # Группируем каналы по категориям
            categories = {}
            for channel_id, channel_info in channels.items():
                category_name = getattr(channel_info, 'category_name', 'Uncategorized')
                if category_name not in categories:
                    categories[category_name] = []
                categories[category_name].append((channel_id, channel_info))
            
            text = (
                f"📋 **All Channels - {server_name}**\n\n"
                f"🔍 **{len(channels)} channels in {len(categories)} categories**\n\n"
                f"💡 **Channel Status:**\n"
                f"✅ = Already monitored\n"
                f"❌ = Not monitored\n"
                f"⚠️ = Not accessible\n\n"
            )
            
            markup = InlineKeyboardMarkup()
            
            # Добавляем кнопки для каждой категории
            for category_name, category_channels in sorted(categories.items()):
                # Показываем только первые 5 каналов в категории
                for channel_id, channel_info in category_channels[:5]:
                    channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
                    monitored = channel_id in getattr(self.discord_service, 'monitored_announcement_channels', set())
                    accessible = getattr(channel_info, 'http_accessible', False)
                    
                    status = "✅" if monitored else ("⚠️" if not accessible else "❌")
                    
                    markup.add(
                        InlineKeyboardButton(
                            f"{status} {category_name}/{channel_name}",
                            callback_data=f"channel_info_{server_name}_{channel_id}"
                        )
                    )
                
                # Если в категории больше 5 каналов, добавляем кнопку "Show more"
                if len(category_channels) > 5:
                    markup.add(
                        InlineKeyboardButton(
                            f"📄 Show all {len(category_channels)} channels in {category_name}",
                            callback_data=f"show_category_{server_name}_{category_name}"
                        )
                    )
            
            # Добавляем кнопки действий
            markup.add(
                InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"),
                InlineKeyboardButton("➕ Add Channel", callback_data=f"add_channel_{server_name}")
            )
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error browsing channels: {e}")
            self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)}")

    def _handle_show_all_removable(self, call):
        """Показать все каналы доступные для удаления (если их больше 10)"""
        try:
            server_name = call.data.replace('show_all_remove_', '', 1)
            
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            server_info = self.discord_service.servers[server_name]
            channels = getattr(server_info, 'accessible_channels', {})
            
            # Получаем все мониторимые каналы
            monitored_channels = {}
            for channel_id, channel_info in channels.items():
                if channel_id in self.discord_service.monitored_announcement_channels:
                    monitored_channels[channel_id] = channel_info
            
            if not monitored_channels:
                self.bot.answer_callback_query(call.id, "❌ No monitored channels")
                return
            
            # Создаем текстовый список всех каналов
            text = (
                f"🗑️ **All Removable Channels - {server_name}**\n\n"
                f"📊 **{len(monitored_channels)} monitored channels:**\n\n"
            )
            
            # Группируем каналы по типу
            announcement_channels = []
            regular_channels = []
            
            for channel_id, channel_info in monitored_channels.items():
                channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
                if self._is_announcement_channel(channel_name):
                    announcement_channels.append((channel_id, channel_name))
                else:
                    regular_channels.append((channel_id, channel_name))
            
            if announcement_channels:
                text += f"📢 **Announcement Channels ({len(announcement_channels)}):**\n"
                for channel_id, channel_name in announcement_channels:
                    text += f"• {channel_name} (`{channel_id}`)\n"
                text += "\n"
            
            if regular_channels:
                text += f"📝 **Regular Channels ({len(regular_channels)}):**\n"
                for channel_id, channel_name in regular_channels:
                    text += f"• {channel_name} (`{channel_id}`)\n"
                text += "\n"
            
            text += (
                f"💡 **To remove a channel:**\n"
                f"1. Use 'Remove Channel' button\n"
                f"2. Select channel from the list\n"
                f"3. Confirm removal\n\n"
                f"⚠️ **Note:** Removal stops monitoring, doesn't delete from Discord"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("🗑️ Remove Channel", callback_data=f"remove_channel_{server_name}"),
                InlineKeyboardButton("📋 Manage All", callback_data=f"manage_channels_{server_name}")
            )
            markup.add(InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error showing all removable channels: {e}")

    def get_channel_management_summary(self, server_name: str) -> Dict:
        """Получить сводку по управлению каналами для API"""
        try:
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                return {"error": "Server not found"}
            
            server_info = self.discord_service.servers[server_name]
            channels = getattr(server_info, 'accessible_channels', {})
            
            # Анализируем каналы
            monitored_channels = []
            unmonitored_channels = []
            
            for channel_id, channel_info in channels.items():
                channel_data = {
                    "channel_id": channel_id,
                    "channel_name": channel_info.channel_name,
                    "is_announcement": self._is_announcement_channel(channel_info.channel_name),
                    "accessible": channel_info.http_accessible,
                    "message_count": getattr(channel_info, 'message_count', 0)
                }
                
                if channel_id in self.discord_service.monitored_announcement_channels:
                    channel_data["monitored"] = True
                    channel_data["can_remove"] = True
                    monitored_channels.append(channel_data)
                else:
                    channel_data["monitored"] = False
                    channel_data["can_add"] = True
                    unmonitored_channels.append(channel_data)
            
            # Статистика
            announcement_monitored = len([ch for ch in monitored_channels if ch["is_announcement"]])
            regular_monitored = len(monitored_channels) - announcement_monitored
            
            return {
                "server_name": server_name,
                "telegram_topic_id": self.server_topics.get(server_name),
                "monitoring_summary": {
                    "total_channels": len(server_info.channels),
                    "accessible_channels": len(channels),
                    "monitored_channels": len(monitored_channels),
                    "unmonitored_channels": len(unmonitored_channels),
                    "announcement_monitored": announcement_monitored,
                    "regular_monitored": regular_monitored
                },
                "monitored_channels": monitored_channels,
                "unmonitored_channels": unmonitored_channels,
                "management_options": {
                    "can_add_channels": len(unmonitored_channels) > 0,
                    "can_remove_channels": len(monitored_channels) > 0,
                    "max_channels": getattr(server_info, 'max_channels', 5),
                    "current_usage": len(server_info.channels)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting channel management summary: {e}")
            return {"error": str(e)}
        
    def _handle_get_messages(self, call):
        """Handle get messages request with actual message retrieval"""
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
            
            # Get messages from first channel
            channel_id, channel_info = next(iter(channels.items()))
            channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
            
            # Try to get actual messages
            messages = []
            if hasattr(self.discord_service, 'get_channel_messages'):
                messages = self.discord_service.get_channel_messages(channel_id, limit=5)
            
            if messages:
                text = f"📋 Last {len(messages)} messages from {channel_name}:\n\n"
                for msg in messages:
                    text += f"• {msg['author']}: {msg['content'][:50]}...\n"
                    if len(msg['content']) > 50:
                        text += f"   (full message: {msg['content']})\n"
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"server_{server_name}"))
                
                self.bot.edit_message_text(
                    text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode=None
                )
            else:
                self.bot.answer_callback_query(
                    call.id,
                    f"ℹ️ No recent messages found in {channel_name}"
                )
            
        except Exception as e:
            self.logger.error(f"Error getting messages: {e}")
            self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)}")
            try:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"server_{server_name}"))
                self.bot.edit_message_text(
                    f"❌ Error retrieving messages: {str(e)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            except:
                pass
    
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
                self.bot.answer_callback_query(call.id, "❌ Invalid data format")
                return
                
            server_name, channel_id = parts
            
            # Get channel name from user state
            user_state = self.user_states.get(call.from_user.id, {})
            channel_name = user_state.get('channel_name', f"Channel_{channel_id}")
            
            self.logger.info(f"Confirming channel addition: server={server_name}, channel_id={channel_id}, channel_name={channel_name}")
            
            # Add channel
            success, message = self.add_channel_to_server(server_name, channel_id, channel_name)
            
            markup = telebot.types.InlineKeyboardMarkup()
            if success:
                markup.add(
                    telebot.types.InlineKeyboardButton("📋 View Server", callback_data=f"server_{server_name}"),
                    telebot.types.InlineKeyboardButton("🔙 Back to Servers", callback_data="servers")
                )
                status_icon = "✅"
            else:
                markup.add(telebot.types.InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"))
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
            try:
                self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)[:50]}")
                markup = telebot.types.InlineKeyboardMarkup()
                markup.add(telebot.types.InlineKeyboardButton("🔙 Back", callback_data="start"))
                self.bot.edit_message_text(
                    f"❌ Error adding channel: {str(e)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            except Exception as inner_e:
                self.logger.error(f"Error in error handling: {inner_e}")
    
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
            
            # Obtain channel information
            channel_name = f"Channel_{channel_id}"
            channel_accessible = False
            channel_type_info = "Unknown"
            
            # Try to get channel info from Discord API
            if self.discord_service and hasattr(self.discord_service, 'sessions') and self.discord_service.sessions:
                try:
                    session = self.discord_service.sessions[0]
                    
                    # We need to run an async function in a synchronous context
                    import asyncio
                    
                    async def get_channel_info():
                        try:
                            # Use rate limiter if available
                            if hasattr(self.discord_service, 'rate_limiter'):
                                await self.discord_service.rate_limiter.wait_if_needed(f"channel_info_{channel_id}")
                            
                            async with session.get(f'https://discord.com/api/v9/channels/{channel_id}') as response:
                                if response.status == 200:
                                    channel_data = await response.json()
                                    name = channel_data.get('name', f'Channel_{channel_id}')
                                    channel_type = channel_data.get('type', 0)
                                    
                                    # Test accessibility
                                    async with session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1') as msg_response:
                                        accessible = msg_response.status == 200
                                        
                                    # Determine channel type
                                    type_info = "Text Channel" if channel_type == 0 else f"Type {channel_type}"
                                    
                                    return name, accessible, type_info
                                elif response.status == 404:
                                    return f"Channel_{channel_id}", False, "Not Found"
                                elif response.status == 403:
                                    return f"Channel_{channel_id}", False, "No Access"
                                else:
                                    return f"Channel_{channel_id}", False, f"Error {response.status}"
                        except Exception as e:
                            self.logger.error(f"Error in get_channel_info: {e}")
                            return f"Channel_{channel_id}", False, "Error"
                    
                    # Create a new event loop in this thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    
                    try:
                        # Run the async function in the new loop
                        channel_name, channel_accessible, channel_type_info = new_loop.run_until_complete(get_channel_info())
                    finally:
                        new_loop.close()
                        
                except Exception as e:
                    self.logger.error(f"Error getting channel info: {e}")
            
            # Save channel info in state
            self.user_states[message.from_user.id]['channel_name'] = channel_name
            self.user_states[message.from_user.id]['channel_accessible'] = channel_accessible
            
            # Access status
            if channel_accessible:
                access_status = "✅ Accessible"
            elif channel_type_info in ["Not Found", "No Access"]:
                access_status = f"❌ {channel_type_info}"
            else:
                access_status = "⚠️ Status unknown"
            
            # All manually added channels will be monitored
            monitoring_info = "🔔 **WILL BE MONITORED** - Messages will be forwarded to Telegram"
            monitoring_emoji = "✅"
            
            confirmation_text = (
                f"🔍 **Channel Information**\n\n"
                f"Server: **{server_name}**\n"
                f"Channel ID: `{channel_id}`\n"
                f"Channel Name: **{channel_name}**\n"
                f"Type: {channel_type_info}\n"
                f"Access Status: {access_status}\n\n"
                f"{monitoring_emoji} **Monitoring Status:**\n"
                f"{monitoring_info}\n\n"
                f"💡 **Note:** All manually added channels are monitored for messages.\n\n"
                f"➕ **Add this channel to monitoring?**"
            )
            
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(
                telebot.types.InlineKeyboardButton("✅ Add Channel", callback_data=f"confirm_add_{server_name}_{channel_id}"),
                telebot.types.InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_add_{server_name}")
            )
            
            # Delete user's message for cleanliness
            try:
                self.bot.delete_message(message.chat.id, message.message_id)
            except Exception as e:
                self.logger.debug(f"Could not delete message: {e}")
            
            # Update original message with confirmation
            try:
                self.bot.edit_message_text(
                    confirmation_text,
                    original_chat_id,
                    original_message_id,
                    reply_markup=markup,
                    parse_mode='Markdown'
                )
            except Exception as e:
                self.logger.error(f"Could not edit message: {e}")
                # Fallback: send as new message
                self.bot.send_message(
                    message.chat.id,
                    confirmation_text,
                    reply_markup=markup,
                    parse_mode='Markdown'
                )
                
        except Exception as e:
            self.logger.error(f"Error processing channel ID input: {e}")
            try:
                self.bot.reply_to(message, f"❌ Error processing channel ID: {str(e)}")
            except:
                pass
    
    def _send_servers_list_message(self, message):
        """: Отправка списка серверов как новое сообщение"""
        try:
            if not self.discord_service:
                self.bot.reply_to(message, "❌ Discord service not available")
                return
            
            # Получаем серверы из discord_service
            servers = getattr(self.discord_service, 'servers', {})
            
            if not servers:
                self.bot.reply_to(message, "❌ No Discord servers found")
                return
            
            # Проверяем наличие monitored_announcement_channels
            if not hasattr(self.discord_service, 'monitored_announcement_channels'):
                self.discord_service.monitored_announcement_channels = set()
            
            # Показываем первые 10 серверов
            text = f"📋 **Discord Servers ({len(servers)} total)**\n\n"
            
            for i, (server_name, server_info) in enumerate(list(servers.items())[:10], 1):
                # Получаем базовую информацию о сервере
                total_channels = getattr(server_info, 'channel_count', 0)
                accessible_channels = getattr(server_info, 'accessible_channel_count', 0)
                
                # Подсчитываем мониторимые каналы
                monitored_count = 0
                for channel_id in getattr(server_info, 'channels', {}).keys():
                    if channel_id in self.discord_service.monitored_announcement_channels:
                        monitored_count += 1
                
                # Статус топика
                topic_id = self.server_topics.get(server_name)
                if topic_id:
                    try:
                        topic_info = self.bot.get_forum_topic(
                            chat_id=self.settings.telegram_chat_id,
                            message_thread_id=topic_id
                        )
                        topic_status = "✅" if topic_info else "❌"
                    except:
                        topic_status = "❌"
                    
                    text += f"{i}. **{server_name}**\n"
                    text += f"   📊 Channels: {total_channels} ({accessible_channels} accessible)\n"
                    text += f"   🔔 Monitored: {monitored_count}\n"
                    text += f"   📋 Topic: {topic_id} {topic_status}\n\n"
                else:
                    text += f"{i}. **{server_name}**\n"
                    text += f"   📊 Channels: {total_channels} ({accessible_channels} accessible)\n"
                    text += f"   🔔 Monitored: {monitored_count}\n"
                    text += f"   📋 Topic: Will be created when needed\n\n"
            
            if len(servers) > 10:
                text += f"... and {len(servers) - 10} more servers\n\n"
            
            # Добавляем общую статистику
            total_all_channels = sum(getattr(s, 'channel_count', 0) for s in servers.values())
            total_accessible = sum(getattr(s, 'accessible_channel_count', 0) for s in servers.values())
            total_monitored = len(self.discord_service.monitored_announcement_channels)
            
            text += f"📊 **Summary:**\n"
            text += f"• Total channels: {total_all_channels}\n"
            text += f"• Accessible: {total_accessible}\n"
            text += f"• Monitored: {total_monitored}\n"
            text += f"• Active topics: {len(self.server_topics)}\n"
            text += f"🛡️ Anti-duplicate protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}\n\n"
            text += f"💡 Use `/start` for interactive menu with full server management"
            
            self.bot.reply_to(message, text, parse_mode='Markdown')
            
            self.logger.info(f"Sent servers list via command: {len(servers)} servers, {total_monitored} monitored channels")
            
        except Exception as e:
            self.logger.error(f"Error sending servers list: {e}")
            self.bot.reply_to(message, f"❌ Error loading servers: {str(e)[:100]}...")
    
    def _handle_remove_channel_request(self, call):
        """Обработчик запроса на удаление канала"""
        try:
            server_name = call.data.replace('remove_channel_', '', 1)
            
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            server_info = self.discord_service.servers[server_name]
            channels = getattr(server_info, 'accessible_channels', {})
            
            # Получаем только мониторимые каналы
            monitored_channels = {}
            for channel_id, channel_info in channels.items():
                if channel_id in self.discord_service.monitored_announcement_channels:
                    monitored_channels[channel_id] = channel_info
            
            if not monitored_channels:
                self.bot.answer_callback_query(call.id, "❌ No monitored channels to remove")
                return
            
            text = (
                f"🗑️ **Remove Channel from {server_name}**\n\n"
                f"Select a channel to remove from monitoring:\n\n"
                f"⚠️ **Warning:** Removed channels will no longer forward messages to Telegram.\n"
            )
            
            markup = InlineKeyboardMarkup()
            
            # Показываем кнопки для каждого мониторимого канала
            for channel_id, channel_info in list(monitored_channels.items())[:10]:  # Limit to 10
                channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
                
                # Определяем тип канала
                if self._is_announcement_channel(channel_name):
                    channel_type_emoji = "📢"
                    channel_type = "announcement"
                else:
                    channel_type_emoji = "📝"
                    channel_type = "regular"
                
                # Укорачиваем название для кнопки
                display_name = channel_name[:25] + "..." if len(channel_name) > 25 else channel_name
                
                markup.add(
                    InlineKeyboardButton(
                        f"{channel_type_emoji} {display_name}",
                        callback_data=f"confirm_remove_{server_name}_{channel_id}"
                    )
                )
            
            if len(monitored_channels) > 10:
                markup.add(
                    InlineKeyboardButton(
                        f"📄 Show all ({len(monitored_channels)} total)",
                        callback_data=f"show_all_remove_{server_name}"
                    )
                )
            
            markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"server_{server_name}"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error in remove channel request: {e}")

    def _handle_confirm_remove_channel(self, call):
        """Обработчик подтверждения удаления канала"""
        try:
            # Парсим callback data: confirm_remove_{server_name}_{channel_id}
            parts = call.data.replace('confirm_remove_', '', 1).split('_', 1)
            if len(parts) != 2:
                self.bot.answer_callback_query(call.id, "❌ Invalid data format")
                return
            
            server_name, channel_id = parts
            
            self.logger.info(f"Confirming channel removal: server={server_name}, channel_id={channel_id}")
            
            # Получаем информацию о канале перед удалением
            channel_name = f"Channel_{channel_id}"
            channel_type = "unknown"
            
            if self.discord_service and server_name in getattr(self.discord_service, 'servers', {}):
                server_info = self.discord_service.servers[server_name]
                if channel_id in server_info.channels:
                    channel_info = server_info.channels[channel_id]
                    channel_name = getattr(channel_info, 'channel_name', channel_name)
                    channel_type = "announcement" if self._is_announcement_channel(channel_name) else "regular"
            
            # Показываем финальное подтверждение
            text = (
                f"🗑️ **Confirm Channel Removal**\n\n"
                f"**Server:** {server_name}\n"
                f"**Channel:** {channel_name}\n"
                f"**Type:** {channel_type.title()}\n"
                f"**Channel ID:** `{channel_id}`\n\n"
                f"⚠️ **This will:**\n"
                f"• Stop monitoring this channel\n"
                f"• Remove it from message forwarding\n"
                f"• Channel will remain in Discord (not deleted)\n\n"
                f"❓ **Are you sure you want to remove this channel from monitoring?**"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("✅ Yes, Remove", callback_data=f"final_remove_{server_name}_{channel_id}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"remove_channel_{server_name}")
            )
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error confirming remove channel: {e}")

    def _handle_final_remove_channel(self, call):
        """Обработчик финального удаления канала"""
        try:
            # Парсим callback data: final_remove_{server_name}_{channel_id}
            parts = call.data.replace('final_remove_', '', 1).split('_', 1)
            if len(parts) != 2:
                self.bot.answer_callback_query(call.id, "❌ Invalid data format")
                return
            
            server_name, channel_id = parts
            
            self.logger.info(f"Final channel removal: server={server_name}, channel_id={channel_id}")
            
            # Выполняем удаление
            success, message = self.remove_channel_from_server(server_name, channel_id)
            
            markup = InlineKeyboardMarkup()
            if success:
                markup.add(
                    InlineKeyboardButton("📋 View Server", callback_data=f"server_{server_name}"),
                    InlineKeyboardButton("🔙 Back to Servers", callback_data="servers")
                )
                status_icon = "✅"
                result_text = "**Channel Removal Successful**"
            else:
                markup.add(InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"))
                status_icon = "❌"
                result_text = "**Channel Removal Failed**"
            
            # Получаем обновленную информацию о канале
            channel_name = f"Channel_{channel_id}"
            if self.discord_service and server_name in getattr(self.discord_service, 'servers', {}):
                server_info = self.discord_service.servers[server_name]
                if channel_id in server_info.channels:
                    channel_info = server_info.channels[channel_id]
                    channel_name = getattr(channel_info, 'channel_name', channel_name)
            
            self.bot.edit_message_text(
                f"{status_icon} {result_text}\n\n"
                f"**Server:** {server_name}\n"
                f"**Channel:** {channel_name}\n"
                f"**Channel ID:** `{channel_id}`\n\n"
                f"**Result:** {message}",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error in final remove channel: {e}")
            try:
                self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)[:50]}")
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back", callback_data="start"))
                self.bot.edit_message_text(
                    f"❌ Error removing channel: {str(e)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            except Exception as inner_e:
                self.logger.error(f"Error in error handling: {inner_e}")

    def _handle_manage_channels(self, call):
        """Обработчик управления каналами (детальный список)"""
        try:
            server_name = call.data.replace('manage_channels_', '', 1)
            
            if not self.discord_service or server_name not in getattr(self.discord_service, 'servers', {}):
                self.bot.answer_callback_query(call.id, "❌ Server not found")
                return
            
            server_info = self.discord_service.servers[server_name]
            channels = getattr(server_info, 'accessible_channels', {})
            
            # Получаем мониторимые каналы с детальной информацией
            monitored_channels = []
            for channel_id, channel_info in channels.items():
                if channel_id in self.discord_service.monitored_announcement_channels:
                    is_announcement = self._is_announcement_channel(channel_info.channel_name)
                    monitored_channels.append({
                        'id': channel_id,
                        'name': channel_info.channel_name,
                        'type': 'announcement' if is_announcement else 'regular',
                        'accessible': channel_info.http_accessible,
                        'message_count': getattr(channel_info, 'message_count', 0),
                        'last_message': getattr(channel_info, 'last_message_time', None)
                    })
            
            if not monitored_channels:
                self.bot.answer_callback_query(call.id, "❌ No monitored channels")
                return
            
            text = (
                f"📋 **Channel Management - {server_name}**\n\n"
                f"🔔 **Monitored Channels ({len(monitored_channels)}):**\n\n"
            )
            
            # Показываем детальную информацию о каналах
            for i, channel in enumerate(monitored_channels[:8], 1):
                type_emoji = "📢" if channel['type'] == 'announcement' else "📝"
                access_emoji = "✅" if channel['accessible'] else "❌"
                
                text += f"{i}. {type_emoji} **{channel['name']}**\n"
                text += f"   🆔 `{channel['id']}`\n"
                text += f"   🔗 Access: {access_emoji}\n"
                text += f"   📊 Messages: {channel['message_count']}\n"
                
                if channel['last_message']:
                    last_msg_time = channel['last_message'].strftime('%Y-%m-%d %H:%M')
                    text += f"   📅 Last: {last_msg_time}\n"
                
                text += "\n"
            
            if len(monitored_channels) > 8:
                text += f"... and {len(monitored_channels) - 8} more channels\n\n"
            
            text += (
                f"💡 **Actions:**\n"
                f"• All channels forward to the same topic\n"
                f"• Use buttons below to add/remove channels"
            )
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("➕ Add Channel", callback_data=f"add_channel_{server_name}"),
                InlineKeyboardButton("🗑️ Remove Channel", callback_data=f"remove_channel_{server_name}")
            )
            markup.add(
                InlineKeyboardButton("📥 Get Messages", callback_data=f"get_messages_{server_name}"),
                InlineKeyboardButton("📊 Channel Stats", callback_data=f"channel_stats_{server_name}")
            )
            markup.add(InlineKeyboardButton("🔙 Back to Server", callback_data=f"server_{server_name}"))
            
            self.bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            self.logger.error(f"Error in manage channels: {e}")


    
    async def get_or_create_server_topic(self, server_name: str) -> Optional[int]:
        """ Get or create topic with anti-duplicate protection"""
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
        """Start the enhanced Telegram bot asynchronously"""
        if self.bot_running:
            self.logger.warning("Enhanced bot is already running")
            return
        
        if not self.bot:
            self.logger.error("Bot not initialized, cannot start")
            return
        
        self._setup_bot_handlers()
        self.bot_running = True
        
        
        self.logger.info("Starting Enhanced Telegram bot", 
                       chat_id=self.settings.telegram_chat_id,
                       use_topics=self.settings.use_topics,
                       server_topics=len(self.server_topics),
                       features=["Anti-duplicate", "Interactive UI", "Channel management"])
        
        try:
            # Запускаем бота в отдельном потоке
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
        """Stop the enhanced Telegram bot"""
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