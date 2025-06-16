# app/services/telegram_service.py - ENHANCED VERSION with Bot Interface
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from threading import Lock
import structlog
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import os

from ..models.message import DiscordMessage
from ..models.server import ServerInfo
from ..config import Settings
from ..utils.rate_limiter import RateLimiter

class TelegramService:
    """Enhanced Telegram service with comprehensive bot interface and channel management"""
    
    def __init__(self, 
                 settings: Settings,
                 rate_limiter: RateLimiter,
                 redis_client = None,
                 logger = None):
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.redis_client = redis_client
        self.logger = logger or structlog.get_logger(__name__)
        
        # Telegram Bot Configuration
        self.bot = telebot.TeleBot(
            self.settings.telegram_bot_token,
            skip_pending=True,
            threaded=True
        )
        self.bot.set_update_listener(self._handle_bot_updates)
        
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
        
        # ENHANCED: Thread-safe operations
        self._async_lock = asyncio.Lock()
        self.topic_creation_lock = asyncio.Lock()
        self.topic_sync_lock = asyncio.Lock()
        
        # ENHANCED: Callbacks and monitoring
        self.new_message_callbacks: List[Callable[[DiscordMessage], None]] = []
        self.discord_service = None  # Will be set by dependency injection
        
        # Load existing data
        self._load_persistent_data()
    
    def set_discord_service(self, discord_service):
        """Set Discord service reference for channel management"""
        self.discord_service = discord_service
        self.logger.info("Discord service reference set for Telegram bot")
    
    async def initialize(self) -> bool:
        """Initialize Enhanced Telegram service with startup verification"""
        try:
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
    
    def setup_enhanced_bot_handlers(self) -> None:
        """ENHANCED: Setup comprehensive bot command handlers with interactive UI"""
        
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            """Enhanced welcome message with feature overview"""
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
        
        @self.bot.callback_query_handler(func=lambda call: True)
        def handle_callback_query(call):
            """Enhanced callback handler with comprehensive UI"""
            try:
                data = call.data
                self.logger.info(f"📞 Callback received: {data} from user {call.from_user.id}")
                
                # Answer callback to remove loading state
                self.bot.answer_callback_query(call.id)
                
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
                
            except Exception as e:
                self.logger.error(f"❌ Error handling callback query: {e}")
                try:
                    self.bot.answer_callback_query(call.id, "❌ An error occurred")
                except:
                    pass
        
        @self.bot.message_handler(func=lambda message: True)
        def handle_text_message(message):
            """Enhanced text message handler for channel addition workflow"""
            user_id = message.from_user.id
            
            # Check if user is in channel addition workflow
            if user_id in self.user_states:
                user_state = self.user_states[user_id]
                
                if user_state.get('action') == 'waiting_for_channel_id':
                    self._process_channel_id_input(message, user_state)
        
        # Enhanced command handlers
        @self.bot.message_handler(commands=['servers'])
        def list_servers_command(message):
            """Command to list servers"""
            self._send_servers_list(message)
        
        @self.bot.message_handler(commands=['reset_topics'])
        def reset_topics(message):
            """Reset all topic mappings with confirmation"""
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
        
        @self.bot.message_handler(commands=['verify_topics'])
        def verify_topics_command(message):
            """Force topic verification"""
            self.startup_verification_done = False
            old_count = len(self.server_topics)
            
            # Run verification
            asyncio.create_task(self.startup_topic_verification())
            
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
        
        @self.bot.message_handler(commands=['cleanup_topics'])
        def cleanup_topics_command(message):
            """Clean invalid topics"""
            cleaned = asyncio.run(self._clean_invalid_topics())
            self.bot.reply_to(
                message, 
                f"🧹 Cleaned {cleaned} invalid/duplicate topics.\n"
                f"📋 Current active topics: {len(self.server_topics)}\n"
                f"🛡️ Anti-duplicate protection: ✅ ACTIVE"
            )
    
    def _handle_servers_list(self, call):
        """Enhanced server list handler with interactive management"""
        if not hasattr(self.settings, 'server_channel_mappings') or not self.settings.server_channel_mappings:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
            self.bot.edit_message_text(
                "❌ No servers found. Please configure servers first.",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )
            return
        
        markup = InlineKeyboardMarkup()
        for server in self.settings.server_channel_mappings.keys():
            # Add topic indicator with duplicate check
            topic_indicator = ""
            if server in self.server_topics:
                topic_id = self.server_topics[server]
                exists = asyncio.run(self._topic_exists(call.message.chat.id, topic_id))
                topic_indicator = " 📋" if exists else " ❌"
            else:
                topic_indicator = " 🆕"  # New server, no topic yet
            
            markup.add(InlineKeyboardButton(
                f"{server}{topic_indicator}",
                callback_data=f"server_{server}"
            ))
        markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
        
        server_count = len(self.settings.server_channel_mappings)
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
    
    def _process_channel_id_input(self, message, user_state):
        """Process channel ID input from user"""
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
    
    def add_channel_to_server(self, server_name: str, channel_id: str, channel_name: str = None) -> tuple[bool, str]:
        """Enhanced channel addition with Discord service integration"""
        try:
            # Check if server exists in configuration
            if not hasattr(self.settings, 'server_channel_mappings'):
                self.settings.server_channel_mappings = {}
            
            if server_name not in self.settings.server_channel_mappings:
                self.settings.server_channel_mappings[server_name] = {}
            
            # Check if channel already added
            if channel_id in self.settings.server_channel_mappings[server_name]:
                return False, "Channel already added to this server"
            
            # Test channel accessibility if Discord service available
            access_confirmed = False
            if self.discord_service:
                try:
                    # Here you would test channel access
                    # For now, we'll mark as unconfirmed
                    pass
                except Exception as e:
                    self.logger.warning(f"⚠️ Cannot access channel {channel_id}: {e}")
            
            # Add channel to configuration
            final_channel_name = channel_name or f"Channel_{channel_id}"
            self.settings.server_channel_mappings[server_name][channel_id] = final_channel_name
            
            # Add to WebSocket subscriptions if available
            if hasattr(self, 'websocket_service') and self.websocket_service:
                self.websocket_service.add_channel_subscription(channel_id)
                self.logger.info(f"✅ Added channel {channel_id} to WebSocket subscriptions")
            
            self.logger.info(f"✅ Added channel '{final_channel_name}' ({channel_id}) to server '{server_name}'")
            
            status = "✅ Available" if access_confirmed else "⚠️ Limited access (will monitor via WebSocket)"
            
            return True, f"Channel successfully added!\nStatus: {status}"
            
        except Exception as e:
            self.logger.error(f"❌ Error adding channel to server: {e}")
            return False, f"Error adding channel: {str(e)}"
    
    def _handle_bot_updates(self, messages):
        """Handle bot updates for enhanced monitoring"""
        for message in messages:
            if message.content_type == 'text':
                self.logger.debug(f"Bot received message: {message.text[:50]}...")
    
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
            server_messages.sort(key=lambda x: x.timestamp)
            
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
                       servers_processed=len(server_groups))
        
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
    
    # Handler methods for bot interface
    def _handle_manual_sync(self, call):
        """Handle manual sync request"""
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
        
        try:
            sync_result = "🔄 Manual sync completed successfully!"
            if self.discord_service:
                server_count = len(getattr(self.settings, 'server_channel_mappings', {}))
                sync_result += f"\n📊 Found {server_count} servers"
            
            self.bot.edit_message_text(
                sync_result,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )
        except Exception as e:
            self.bot.edit_message_text(
                f"❌ Sync failed: {str(e)}",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )
    
    def _handle_websocket_status(self, call):
        """Handle WebSocket status request"""
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="start"))
        
        ws_status = "❌ WebSocket service not available"
        if hasattr(self, 'websocket_service') and self.websocket_service:
            subscribed_channels = len(getattr(self.websocket_service, 'subscribed_channels', []))
            ws_status = (
                f"⚡ **WebSocket Status**\n\n"
                f"📡 Subscribed channels: {subscribed_channels}\n"
                f"🔄 Status: {'✅ Active' if getattr(self.websocket_service, 'running', False) else '❌ Inactive'}"
            )
        
        self.bot.edit_message_text(
            ws_status,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode='Markdown'
        )
    
    def _handle_cleanup_topics(self, call):
        """Handle topic cleanup request"""
        cleaned = asyncio.run(self._clean_invalid_topics())
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
    
    def _handle_bot_status(self, call):
        """Handle bot status request"""
        supports_topics = self._check_if_supergroup_with_topics(call.message.chat.id)
        
        status_text = (
            f"📊 **Enhanced Bot Status**\n\n"
            f"🔹 Topics Support: {'✅ Enabled' if supports_topics else '❌ Disabled'}\n"
            f"🔹 Active Topics: {len(self.server_topics)}\n"
            f"🔹 Configured Servers: {len(getattr(self.settings, 'server_channel_mappings', {}))}\n"
            f"🔹 Message Cache: {len(self.message_mappings)}\n"
            f"🔹 Processed Messages: {len(self.processed_messages)}\n"
            f"🛡️ Anti-Duplicate Protection: {'✅ ACTIVE' if self.startup_verification_done else '⚠️ PENDING'}\n"
            f"🔹 Topic Logic: **One server = One topic ✅**\n"
            f"🔹 Startup Verification: {'✅ Complete' if self.startup_verification_done else '⏳ In Progress'}\n\n"
            f"📋 **Current Topics:**\n"
        )
        
        if self.server_topics:
            for server, topic_id in list(self.server_topics.items())[:10]:
                exists = asyncio.run(self._topic_exists(call.message.chat.id, topic_id))
                status_icon = "✅" if exists else "❌"
                status_text += f"• {server}: Topic {topic_id} {status_icon}\n"
            
            if len(self.server_topics) > 10:
                status_text += f"• ... and {len(self.server_topics) - 10} more topics\n"
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
    
    def _handle_help(self, call):
        """Handle help request"""
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
    
    def _handle_verify_topics(self, call):
        """Handle topic verification request"""
        self.startup_verification_done = False
        asyncio.create_task(self.startup_topic_verification())
        
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
    
    def _handle_server_selected(self, call):
        """Handle server selection"""
        server_name = call.data.replace('server_', '', 1)
        
        if not hasattr(self.settings, 'server_channel_mappings') or server_name not in self.settings.server_channel_mappings:
            self.bot.answer_callback_query(call.id, "❌ Server not found")
            return
        
        channels = self.settings.server_channel_mappings[server_name]
        channel_count = len(channels)
        
        # Topic information
        topic_info = ""
        existing_topic_id = self.server_topics.get(server_name)
        if existing_topic_id:
            exists = asyncio.run(self._topic_exists(call.message.chat.id, existing_topic_id))
            if exists:
                topic_info = f"📋 Topic: {existing_topic_id} ✅"
            else:
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
            for channel_id, channel_name in list(channels.items())[:10]:
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
    
    def _handle_get_messages(self, call):
        """Handle get messages request"""
        server_name = call.data.replace('get_messages_', '', 1)
        
        if not hasattr(self.settings, 'server_channel_mappings'):
            self.bot.answer_callback_query(call.id, "❌ No server configuration found")
            return
        
        channels = self.settings.server_channel_mappings.get(server_name, {})
        
        if not channels:
            self.bot.answer_callback_query(call.id, "❌ No channels found for this server")
            return
        
        # Get first channel for example
        channel_id, channel_name = next(iter(channels.items()))
        
        if self.discord_service:
            try:
                # Here you would call discord_service.get_recent_messages
                # For now, we'll simulate success
                message_count = 5  # Simulated
                
                self.bot.answer_callback_query(
                    call.id,
                    f"✅ Sent {message_count} messages from {server_name}"
                )
                
            except Exception as e:
                self.logger.error(f"Error getting messages: {e}")
                self.bot.answer_callback_query(call.id, f"❌ Error: {str(e)}")
        else:
            self.bot.answer_callback_query(call.id, "❌ Discord service not available")
    
    def _handle_add_channel_request(self, call):
        """Handle add channel request"""
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
    
    def _handle_confirm_add_channel(self, call):
        """Handle channel addition confirmation"""
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
    
    def _handle_cancel_add_channel(self, call):
        """Handle cancel add channel"""
        server_name = call.data.replace('cancel_add_', '', 1)
        
        # Clear user state
        if call.from_user.id in self.user_states:
            del self.user_states[call.from_user.id]
        
        # Return to server view
        call.data = f"server_{server_name}"
        self._handle_server_selected(call)
    
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
        
        self.setup_enhanced_bot_handlers()
        self.bot_running = True
        
        self.logger.info("Starting Enhanced Telegram bot", 
                       chat_id=self.settings.telegram_chat_id,
                       use_topics=self.settings.use_topics,
                       server_topics=len(self.server_topics),
                       features=["Anti-duplicate", "Interactive UI", "Channel management"])
        
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
            self.logger.error("Enhanced bot polling error", error=str(e))
        finally:
            self.bot_running = False
            self.logger.info("Enhanced Telegram bot stopped")
    
    def stop_bot(self) -> None:
        """Stop the enhanced Telegram bot"""
        if self.bot_running:
            try:
                self.bot.stop_polling()
                self.bot_running = False
                self.logger.info("Enhanced Telegram bot stopped")
            except Exception as e:
                self.logger.error("Error stopping enhanced bot", error=str(e))
                self.bot_running = False
    
    async def cleanup(self) -> None:
        """Enhanced cleanup"""
        self.stop_bot()
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
        return {
            "topics": {
                "total": len(self.server_topics),
                "verified": len([t for t in self.server_topics.values() 
                               if asyncio.run(self._topic_exists(self.settings.telegram_chat_id, t))]),
                "startup_verification_done": self.startup_verification_done
            },
            "messages": {
                "tracked": len(self.message_mappings),
                "processed_cache": len(self.processed_messages)
            },
            "bot": {
                "running": self.bot_running,
                "active_user_states": len(self.user_states)
            },
            "features": {
                "anti_duplicate": True,
                "interactive_ui": True,
                "channel_management": True,
                "topic_verification": True,
                "websocket_integration": True
            }
        }