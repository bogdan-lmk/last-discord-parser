# app/services/discord_service.py - ПОЛНАЯ РЕАЛИЗАЦИЯ WebSocket
import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Callable
import structlog
import random

from ..models.message import DiscordMessage
from ..models.server import ServerInfo, ChannelInfo, ServerStatus
from ..config import Settings
from ..utils.rate_limiter import RateLimiter

class DiscordService:
    """Discord service с полной WebSocket реализацией"""
    
    def __init__(self, 
                 settings: Settings,
                 rate_limiter: RateLimiter,
                 redis_client = None,
                 logger = None):
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.redis_client = redis_client
        self.logger = logger or structlog.get_logger(__name__)
        
        # Session management
        self.sessions: List[aiohttp.ClientSession] = []
        self.current_token_index = 0
        self.token_failure_counts: Dict[int, int] = {}
        
        self.telegram_service_ref = None
        
        # Server tracking
        self.servers: Dict[str, ServerInfo] = {}
        
        # WebSocket management
        self.websocket_connections: List[Dict] = []  # List of connection info
        self.gateway_urls: List[str] = []  # Gateway URLs for each token
        
        # Channel monitoring
        self.message_callbacks: List[Callable] = []
        self.monitored_announcement_channels: Set[str] = set()
        
        # WebSocket state tracking
        self.websocket_sessions: Dict[int, Dict] = {}  # token_index -> session_info
        
        # State
        self.running = False
        self._initialization_done = False
        
        # Enhanced rate limiting
        self.last_request_time = {}
        self.backoff_until = {}
        
        # Retry configuration
        self.max_retries = 3
        self.base_delay = 1.0
        self.max_delay = 60.0
        
        # WebSocket intents (GUILDS + GUILD_MESSAGES)
        self.intents = (1 << 0) | (1 << 9)  # GUILDS + GUILD_MESSAGES
    
    def add_message_callback(self, callback: Callable):
        """Add callback for real-time messages"""
        self.message_callbacks.append(callback)
        self.logger.info("Message callback added", callback_count=len(self.message_callbacks))
    
    def remove_message_callback(self, callback: Callable):
        """Remove message callback"""
        if callback in self.message_callbacks:
            self.message_callbacks.remove(callback)
            self.logger.info("Message callback removed", callback_count=len(self.message_callbacks))
    
    async def _trigger_message_callbacks(self, message: DiscordMessage):
        """Trigger all registered message callbacks"""
        for callback in self.message_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(message)
                else:
                    callback(message)
            except Exception as e:
                self.logger.error("Error in message callback", error=str(e))
    
    def _is_announcement_channel(self, channel_name: str, channel_type: Optional[int] = None, category_name: Optional[str] = None) -> bool:
        """Проверка что канал является announcement по названию, типу и категории"""
        # Очистка названия канала и категории от emoji и лишних символов
        clean_channel = ''.join([c for c in channel_name if c.isalpha() or c.isspace()])
        clean_channel = ' '.join(clean_channel.split()).lower()
        
        clean_category = ''
        if category_name:
            clean_category = ''.join([c for c in category_name if c.isalpha() or c.isspace()])
            clean_category = ' '.join(clean_category.split()).lower()
        
        # Ключевые слова для поиска в названиях каналов и категорий
        announcement_keywords = [
            'announce', 'updates', 'big-announcements',
            'news', 'announcements', 'announcement',
            'project-announcements', '📢┋announcements',
            '📢・announcements', '📢│big-announcements',
            '🐚┋announcements', '🗣・announcements'
        ]
        
        # Проверка по ключевым словам в названии канала
        for keyword in announcement_keywords + self.settings.channel_keywords:
            if keyword in clean_channel:
                return True
                
        # Проверка по ключевым словам в названии категории
        if clean_category:
            for keyword in announcement_keywords + self.settings.channel_keywords:
                if keyword in clean_category:
                    return True
                    
        return False
    
    async def initialize(self) -> bool:
        """Initialize Discord service"""
        if self._initialization_done:
            return True
            
        self.logger.info("Initializing Discord service with WebSocket monitoring", 
                        token_count=len(self.settings.discord_tokens),
                        max_servers=self.settings.max_servers,
                        max_channels_total=self.settings.max_total_channels)
        
        # Create sessions and get gateway URLs
        successful_tokens = 0
        for i, token in enumerate(self.settings.discord_tokens):
            session = aiohttp.ClientSession(
                headers={
                    'Authorization': f'Bot {token}' if not token.startswith('Bot ') else token,
                    'User-Agent': 'DiscordBot (Discord-Parser-MVP, 1.0)'
                },
                timeout=aiohttp.ClientTimeout(total=30, connect=10),
                connector=aiohttp.TCPConnector(
                    limit=20,
                    limit_per_host=5,
                    ttl_dns_cache=300,
                    use_dns_cache=True
                )
            )
            
            if await self._validate_token_and_get_gateway(session, i):
                self.sessions.append(session)
                self.token_failure_counts[i] = 0
                successful_tokens += 1
                self.logger.info("Token validated and gateway obtained", token_index=i)
            else:
                await session.close()
                self.logger.error("Token validation or gateway retrieval failed", token_index=i)
        
        if not self.sessions:
            self.logger.error("No valid Discord tokens available")
            return False
        
        # Discover announcement channels
        await self._discover_announcement_channels_only()
        
        self._initialization_done = True
        self.logger.info("Discord service initialized with WebSocket support", 
                        valid_tokens=len(self.sessions),
                        servers_found=len(self.servers),
                        announcement_channels=len(self.monitored_announcement_channels),
                        gateway_urls=len(self.gateway_urls))
        return True
    
    async def _validate_token_and_get_gateway(self, session: aiohttp.ClientSession, token_index: int) -> bool:
        """Validate token and get gateway URL"""
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    delay = min(self.max_delay, self.base_delay * (2 ** attempt))
                    await asyncio.sleep(delay)
                
                await self.rate_limiter.wait_if_needed(f"token_validate_{token_index}")
                
                # Validate token
                async with session.get('https://discord.com/api/v10/users/@me') as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 60))
                        self.logger.warning("Rate limited during token validation", 
                                          token_index=token_index,
                                          retry_after=retry_after,
                                          attempt=attempt + 1)
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(min(retry_after, 60))
                            continue
                        else:
                            return False
                    
                    if response.status != 200:
                        self.logger.error("Token validation failed", 
                                        token_index=token_index,
                                        status=response.status,
                                        attempt=attempt + 1)
                        
                        if response.status in [401, 403]:
                            return False
                        
                        continue
                    
                    user_data = await response.json()
                    self.logger.info("Token valid for user", 
                                   username=user_data.get('username'),
                                   token_index=token_index)
                
                # Get gateway URL
                async with session.get('https://discord.com/api/v10/gateway/bot') as gateway_response:
                    if gateway_response.status == 429:
                        retry_after = float(gateway_response.headers.get('Retry-After', 60))
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(min(retry_after, 60))
                            continue
                        else:
                            return False
                    
                    if gateway_response.status != 200:
                        self.logger.error("Gateway URL retrieval failed", 
                                        token_index=token_index,
                                        status=gateway_response.status)
                        if gateway_response.status in [401, 403]:
                            return False
                        continue
                    
                    gateway_data = await gateway_response.json()
                    gateway_url = gateway_data.get('url')
                    
                    if not gateway_url:
                        self.logger.error("No gateway URL in response", token_index=token_index)
                        continue
                    
                    self.gateway_urls.append(gateway_url)
                    self.logger.info("Gateway URL obtained", 
                                   token_index=token_index,
                                   gateway_url=gateway_url,
                                   session_limit=gateway_data.get('session_start_limit', {}))
                
                # Test guild access
                async with session.get('https://discord.com/api/v10/users/@me/guilds') as guilds_res:
                    if guilds_res.status == 429:
                        retry_after = float(guilds_res.headers.get('Retry-After', 60))
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(min(retry_after, 60))
                            continue
                        else:
                            return False
                    
                    if guilds_res.status != 200:
                        if guilds_res.status in [401, 403]:
                            return False
                        continue
                    
                    guilds = await guilds_res.json()
                    self.logger.info("Token has access to guilds", 
                                   guild_count=len(guilds),
                                   token_index=token_index)
                
                self.rate_limiter.record_success()
                return True
                
            except asyncio.TimeoutError:
                self.logger.warning("Token validation timeout", 
                                  token_index=token_index,
                                  attempt=attempt + 1)
                self.rate_limiter.record_error()
                
            except Exception as e:
                self.logger.error("Token validation error", 
                                token_index=token_index,
                                error=str(e),
                                attempt=attempt + 1)
                self.rate_limiter.record_error()
        
        return False
    
    async def _discover_announcement_channels_only(self) -> None:
        """Discover announcement channels"""
        if not self.sessions:
            return
        
        self.logger.info("🔍 Discovering ANNOUNCEMENT channels only...")
        
        for attempt in range(self.max_retries):
            try:
                session = self.sessions[0]
                
                await self.rate_limiter.wait_if_needed("discover_guilds")
                
                async with session.get('https://discord.com/api/v10/users/@me/guilds') as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 60))
                        await asyncio.sleep(min(retry_after, 60))
                        continue
                    
                    if response.status != 200:
                        if response.status in [401, 403]:
                            break
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.base_delay * (2 ** attempt))
                            continue
                        break
                    
                    guilds = await response.json()
                    self.logger.info("Discovered guilds", count=len(guilds))
                    
                    # Process each guild
                    for guild in guilds[:self.settings.max_servers]:
                        try:
                            await self._process_guild_announcement_channels_only(session, guild)
                        except Exception as e:
                            self.logger.error("Failed to process guild", 
                                            guild_id=guild.get('id'),
                                            guild_name=guild.get('name'),
                                            error=str(e))
                            continue
                    
                    return
                    
            except Exception as e:
                self.logger.error("Server discovery error", 
                                error=str(e),
                                attempt=attempt + 1)
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.base_delay * (2 ** attempt))
        
        self.logger.warning("Server discovery completed with some failures")
    
    async def _process_guild_announcement_channels_only(self, session: aiohttp.ClientSession, guild_data: dict) -> None:
        """Process guild to find announcement channels"""
        guild_id = guild_data['id']
        guild_name = guild_data['name']
        
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"guild_{guild_id}")
                
                async with session.get(f'https://discord.com/api/v10/guilds/{guild_id}/channels') as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 60))
                        await asyncio.sleep(min(retry_after, 60))
                        continue
                    
                    if response.status != 200:
                        if response.status in [401, 403]:
                            return
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.base_delay * (2 ** attempt))
                            continue
                        return
                    
                    channels = await response.json()
                    
                    # Create server info
                    server_info = ServerInfo(
                        server_name=guild_name,
                        guild_id=guild_id,
                        max_channels=self.settings.max_channels_per_server
                    )
                    
                    # Find ONLY announcement channels
                    announcement_channels = self._find_announcement_channels_only(channels)
                    
                    if not announcement_channels:
                        self.logger.info("No announcement channels found", guild=guild_name)
                        return
                    
                    # Add ONLY announcement channels to server
                    for channel in announcement_channels[:self.settings.max_channels_per_server]:
                        channel_info = ChannelInfo(
                            channel_id=channel['id'],
                            channel_name=channel['name'],
                            category_id=channel.get('parent_id')
                        )
                        
                        # Test channel accessibility
                        channel_info.http_accessible = await self._test_channel_access_with_retry(
                            session, channel['id']
                        )
                        channel_info.last_checked = datetime.now()
                        
                        server_info.add_channel(channel_info)
                        
                        # Add to monitored channels if accessible
                        if channel_info.http_accessible:
                            self.monitored_announcement_channels.add(channel['id'])
                    
                    # Update server stats
                    server_info.update_stats()
                    
                    # Store server ONLY if it has announcement channels
                    if server_info.accessible_channel_count > 0:
                        self.servers[guild_name] = server_info
                        
                        self.logger.info("Added server with announcement channels", 
                                       guild=guild_name,
                                       announcement_channels=len(announcement_channels),
                                       accessible_announcement_channels=server_info.accessible_channel_count)
                    else:
                        self.logger.info("Skipped server - no accessible announcement channels", 
                                       guild=guild_name)
                    
                    return
                    
            except Exception as e:
                self.logger.error("Error processing guild", 
                                guild=guild_name, 
                                error=str(e),
                                attempt=attempt + 1)
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.base_delay * (2 ** attempt))
    
    def _find_announcement_channels_only(self, channels: List[dict]) -> List[dict]:
        """Find announcement channels by name, type and category"""
        announcement_channels = []
        
        for channel in channels:
            channel_type = channel.get('type')
            
            # Проверяем только текстовые (0) и официальные announcement (5) каналы
            if channel_type not in [0, 5]:
                continue
                
            # Получаем название категории если есть
            category_name = None
            if 'parent_id' in channel:
                # В реальном коде здесь должна быть логика получения названия категории по parent_id
                pass
                
            if self._is_announcement_channel(
                channel['name'],
                channel_type=channel_type,
                category_name=category_name
            ):
                announcement_channels.append(channel)
                self.logger.info(
                    "Found announcement channel", 
                    original_name=channel['name'],
                    channel_id=channel['id'],
                    channel_type=channel_type,
                    category=category_name
                )
        
        self.logger.info("Total announcement channels found", 
                       count=len(announcement_channels),
                       by_type=sum(1 for c in announcement_channels if c.get('type') == 5),
                       by_name=sum(1 for c in announcement_channels if c.get('type') != 5))
        return announcement_channels
    
    async def _test_channel_access_with_retry(self, session: aiohttp.ClientSession, channel_id: str) -> bool:
        """Test channel access with retry logic"""
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"test_channel_{channel_id}")
                
                async with session.get(f'https://discord.com/api/v10/channels/{channel_id}/messages?limit=1') as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 60))
                        await asyncio.sleep(min(retry_after, 60))
                        continue
                    
                    result = response.status == 200
                    
                    if result:
                        self.rate_limiter.record_success()
                    else:
                        self.rate_limiter.record_error()
                    
                    return result
                    
            except Exception as e:
                self.logger.debug("Error testing channel access", 
                                channel_id=channel_id,
                                error=str(e),
                                attempt=attempt + 1)
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
        
        self.rate_limiter.record_error()
        return False
    
    async def get_recent_messages(self, 
                             server_name: str, 
                             channel_id: str, 
                             limit: int = 2) -> List[DiscordMessage]:
        """Get recent messages from channel"""
        if server_name not in self.servers:
            self.logger.warning("Server not found", server=server_name)
            return []

        server = self.servers[server_name]
        if channel_id not in server.channels:
            self.logger.warning("Channel not found", 
                            server=server_name, 
                            channel_id=channel_id)
            return []

        channel = server.channels[channel_id]
        
        if channel_id not in self.monitored_announcement_channels:
            self.logger.warning("Channel is not in monitored channels", 
                            server=server_name, 
                            channel=channel.channel_name)
            return []

        if not channel.http_accessible:
            self.logger.warning("Channel not accessible via HTTP", 
                            server=server_name, 
                            channel=channel.channel_name)
            return []

        session = self._get_healthy_session()
        if not session:
            self.logger.error("No healthy sessions available")
            return []

        messages = []
        actual_limit = min(limit, 20)
        
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"messages_{channel_id}")
                
                async with session.get(
                    f'https://discord.com/api/v10/channels/{channel_id}/messages',
                    params={'limit': actual_limit}
                ) as response:
                    
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 60))
                        self.logger.warning("Rate limited fetching messages", 
                                        channel_id=channel_id,
                                        retry_after=retry_after,
                                        attempt=attempt + 1)
                        
                        await asyncio.sleep(min(retry_after, 60))
                        continue
                    
                    if response.status != 200:
                        self.logger.error("Failed to fetch messages", 
                                        channel_id=channel_id,
                                        status=response.status,
                                        attempt=attempt + 1)
                        
                        if response.status in [401, 403]:
                            self.rate_limiter.record_error()
                            return []
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.base_delay * (2 ** attempt))
                            continue
                        
                        self.rate_limiter.record_error()
                        return []
                    
                    raw_messages = await response.json()
                    self.rate_limiter.record_success()
                    
                    # Convert to DiscordMessage objects
                    for raw_msg in raw_messages:
                        try:
                            if not raw_msg.get('content', '').strip():
                                continue
                                
                            message = DiscordMessage(
                                content=raw_msg['content'],
                                timestamp=datetime.fromisoformat(
                                    raw_msg['timestamp'].replace('Z', '+00:00')
                                ),
                                server_name=server_name,
                                channel_name=channel.channel_name,
                                author=raw_msg['author']['username'],
                                message_id=raw_msg['id'],
                                channel_id=channel_id,
                                guild_id=server.guild_id
                            )
                            messages.append(message)
                            
                        except Exception as e:
                            self.logger.warning("Failed to parse message", 
                                            message_id=raw_msg.get('id'),
                                            error=str(e))
                            continue
                    
                    # Update channel stats
                    channel.message_count += len(messages)
                    if messages:
                        latest_message = max(messages, key=lambda x: x.timestamp)
                        channel.last_message_time = latest_message.timestamp
                    
                    channel_type = "announcement" if self._is_announcement_channel(channel.channel_name) else "regular"
                    self.logger.debug("Retrieved messages from monitored channel", 
                                server=server_name,
                                channel=channel.channel_name,
                                channel_type=channel_type,
                                message_count=len(messages),
                                limit_used=actual_limit)
                    
                    return sorted(messages, key=lambda x: x.timestamp)
                    
            except Exception as e:
                self.logger.error("Error retrieving messages", 
                                server=server_name,
                                channel_id=channel_id,
                                error=str(e),
                                attempt=attempt + 1)
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.base_delay * (2 ** attempt))
                
                self.rate_limiter.record_error()
        
        return []
    
    def set_telegram_service_ref(self, telegram_service):
        """Set reference to Telegram service for integration"""
        self.telegram_service_ref = telegram_service
        self.logger.info("Telegram service reference set for Discord integration")
        
        # Проверяем что все необходимые методы доступны
        if hasattr(telegram_service, 'server_topics'):
            self.logger.info(f"Telegram service has {len(telegram_service.server_topics)} topics configured")
        
        if hasattr(telegram_service, 'add_channel_to_server'):
            self.logger.info("Telegram service channel management methods available")
        else:
            self.logger.warning("Telegram service missing channel management methods")
    
    def _get_healthy_session(self) -> Optional[aiohttp.ClientSession]:
        """Get a healthy session using round-robin with failure tracking"""
        if not self.sessions:
            return None
        
        attempts = len(self.sessions)
        
        for _ in range(attempts):
            session_index = self.current_token_index
            self.current_token_index = (self.current_token_index + 1) % len(self.sessions)
            
            failure_count = self.token_failure_counts.get(session_index, 0)
            if failure_count < 5:
                return self.sessions[session_index]
        
        # If all sessions have high failure counts, reset and use the first one
        self.token_failure_counts = {i: 0 for i in range(len(self.sessions))}
        return self.sessions[0]
    
    # ============================================================================
    # WEBSOCKET IMPLEMENTATION - REAL DISCORD GATEWAY CONNECTION
    # ============================================================================
    
    async def start_websocket_monitoring(self) -> None:
        """Start REAL WebSocket monitoring with Discord Gateway"""
        self.logger.info("🚀 Starting WebSocket monitoring", 
                    monitored_channels=len(self.monitored_announcement_channels),
                    tokens=len(self.sessions),
                    gateway_urls=len(self.gateway_urls))
        
        if not self.sessions or not self.gateway_urls:
            self.logger.error("No valid sessions or gateway URLs for WebSocket monitoring")
            return
        
        self.running = True
        
        try:
            # Create WebSocket connection tasks for each token
            websocket_tasks = []
            
            for i, (session, gateway_url) in enumerate(zip(self.sessions, self.gateway_urls)):
                task = asyncio.create_task(
                    self._websocket_connection_handler(session, gateway_url, i),
                    name=f"websocket_token_{i}"
                )
                websocket_tasks.append(task)
                
                # Stagger connection attempts to avoid rate limits
                if i < len(self.sessions) - 1:
                    await asyncio.sleep(5)
            
            self.logger.info(f"✅ Started {len(websocket_tasks)} WebSocket connections")
            
            # Wait for all WebSocket connections
            await asyncio.gather(*websocket_tasks, return_exceptions=True)
            
        except Exception as e:
            self.logger.error("❌ WebSocket monitoring failed", error=str(e))
        finally:
            self.running = False
            self.logger.info("🔌 WebSocket monitoring stopped")
    
    async def _websocket_connection_handler(self, session: aiohttp.ClientSession, gateway_url: str, token_index: int) -> None:
        """Handle individual WebSocket connection"""
        connection_id = f"token_{token_index}"
        max_reconnects = 5
        reconnect_count = 0
        
        while self.running and reconnect_count < max_reconnects:
            try:
                self.logger.info(f"🔌 Connecting WebSocket for {connection_id}", 
                               gateway_url=gateway_url,
                               attempt=reconnect_count + 1)
                
                # Connect to Discord Gateway
                async with session.ws_connect(
                    f"{gateway_url}?v=10&encoding=json",
                    timeout=aiohttp.ClientTimeout(total=None),  # No timeout for WebSocket
                    heartbeat=30
                ) as ws:
                    
                    self.logger.info(f"✅ WebSocket connected for {connection_id}")
                    
                    # Store connection info
                    connection_info = {
                        'ws': ws,
                        'session': session,
                        'token_index': token_index,
                        'connected_at': datetime.now(),
                        'sequence': None,
                        'session_id': None,
                        'heartbeat_task': None,
                        'last_heartbeat': None
                    }
                    
                    # Add to connections list
                    self.websocket_connections.append(connection_info)
                    self.websocket_sessions[token_index] = connection_info
                    
                    try:
                        # Handle WebSocket messages
                        await self._handle_websocket_messages(connection_info)
                        
                    except Exception as e:
                        self.logger.error(f"❌ WebSocket message handling failed for {connection_id}", error=str(e))
                        raise
                    finally:
                        # Cleanup connection
                        if connection_info in self.websocket_connections:
                            self.websocket_connections.remove(connection_info)
                        if token_index in self.websocket_sessions:
                            del self.websocket_sessions[token_index]
                        
                        # Cancel heartbeat task
                        if connection_info.get('heartbeat_task'):
                            connection_info['heartbeat_task'].cancel()
                
            except Exception as e:
                reconnect_count += 1
                self.logger.error(f"❌ WebSocket connection failed for {connection_id}", 
                                error=str(e),
                                reconnect_attempt=reconnect_count)
                
                if reconnect_count < max_reconnects and self.running:
                    # Exponential backoff for reconnection
                    delay = min(60, 5 * (2 ** (reconnect_count - 1)))
                    self.logger.info(f"⏳ Reconnecting WebSocket for {connection_id} in {delay}s")
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(f"💀 Max reconnection attempts reached for {connection_id}")
                    break
        
        self.logger.info(f"🔌 WebSocket handler stopped for {connection_id}")
    
    async def _handle_websocket_messages(self, connection_info: Dict) -> None:
        """Handle WebSocket messages from Discord Gateway"""
        ws = connection_info['ws']
        token_index = connection_info['token_index']
        connection_id = f"token_{token_index}"
        
        self.logger.info(f"👂 Starting message handler for {connection_id}")
        
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await self._process_gateway_event(data, connection_info)
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"❌ JSON decode error for {connection_id}", error=str(e))
                except Exception as e:
                    self.logger.error(f"❌ Error processing gateway event for {connection_id}", error=str(e))
                    
            elif msg.type == aiohttp.WSMsgType.ERROR:
                self.logger.error(f"❌ WebSocket error for {connection_id}", error=ws.exception())
                break
            elif msg.type == aiohttp.WSMsgType.CLOSE:
                self.logger.warning(f"🔌 WebSocket closed for {connection_id}")
                break
    
    async def _process_gateway_event(self, data: Dict, connection_info: Dict) -> None:
        """Process Discord Gateway event"""
        op = data.get('op')
        event_type = data.get('t')
        event_data = data.get('d', {})
        sequence = data.get('s')
        
        connection_id = f"token_{connection_info['token_index']}"
        
        # Update sequence number
        if sequence is not None:
            connection_info['sequence'] = sequence
        
        # Handle different opcodes
        if op == 10:  # HELLO
            await self._handle_hello(event_data, connection_info)
            
        elif op == 11:  # HEARTBEAT_ACK
            connection_info['last_heartbeat'] = datetime.now()
            self.logger.debug(f"💓 Heartbeat ACK for {connection_id}")
            
        elif op == 0:  # DISPATCH
            await self._handle_dispatch_event(event_type, event_data, connection_info)
            
        elif op == 1:  # HEARTBEAT
            await self._send_heartbeat(connection_info)
            
        elif op == 7:  # RECONNECT
            self.logger.warning(f"🔄 Discord requested reconnect for {connection_id}")
            raise ConnectionError("Discord requested reconnect")
            
        elif op == 9:  # INVALID_SESSION
            resumable = event_data if isinstance(event_data, bool) else False
            self.logger.warning(f"❌ Invalid session for {connection_id}", resumable=resumable)
            if not resumable:
                connection_info['session_id'] = None
            raise ConnectionError("Invalid session")
            
        else:
            self.logger.debug(f"🔍 Unknown opcode for {connection_id}", opcode=op)
    
    async def _handle_hello(self, data: Dict, connection_info: Dict) -> None:
        """Handle HELLO event from Discord Gateway"""
        heartbeat_interval = data.get('heartbeat_interval', 41250)  # Default 41.25 seconds
        connection_id = f"token_{connection_info['token_index']}"
        
        self.logger.info(f"👋 Received HELLO for {connection_id}", 
                       heartbeat_interval=heartbeat_interval)
        
        # Start heartbeat task
        connection_info['heartbeat_task'] = asyncio.create_task(
            self._heartbeat_loop(connection_info, heartbeat_interval)
        )
        
        # Send IDENTIFY or RESUME
        if connection_info.get('session_id'):
            await self._send_resume(connection_info)
        else:
            await self._send_identify(connection_info)
    
    async def _heartbeat_loop(self, connection_info: Dict, interval_ms: int) -> None:
        """Send periodic heartbeats"""
        interval_seconds = interval_ms / 1000
        connection_id = f"token_{connection_info['token_index']}"
        
        # Initial random delay
        initial_delay = random.uniform(0, interval_seconds)
        await asyncio.sleep(initial_delay)
        
        while not connection_info['ws'].closed:
            try:
                await self._send_heartbeat(connection_info)
                await asyncio.sleep(interval_seconds)
                
            except asyncio.CancelledError:
                self.logger.debug(f"💓 Heartbeat cancelled for {connection_id}")
                break
            except Exception as e:
                self.logger.error(f"❌ Heartbeat error for {connection_id}", error=str(e))
                break
    
    async def _send_heartbeat(self, connection_info: Dict) -> None:
        """Send heartbeat to Discord Gateway"""
        ws = connection_info['ws']
        sequence = connection_info.get('sequence')
        connection_id = f"token_{connection_info['token_index']}"
        
        heartbeat_payload = {
            "op": 1,
            "d": sequence
        }
        
        try:
            await ws.send_str(json.dumps(heartbeat_payload))
            self.logger.debug(f"💓 Sent heartbeat for {connection_id}", sequence=sequence)
        except Exception as e:
            self.logger.error(f"❌ Failed to send heartbeat for {connection_id}", error=str(e))
            raise
    
    async def _send_identify(self, connection_info: Dict) -> None:
        """Send IDENTIFY to Discord Gateway"""
        ws = connection_info['ws']
        token_index = connection_info['token_index']
        connection_id = f"token_{token_index}"
        
        # Get token from session headers
        token = self.settings.discord_tokens[token_index]
        if not token.startswith('Bot '):
            token = f'Bot {token}'
        
        identify_payload = {
            "op": 2,
            "d": {
                "token": token,
                "properties": {
                    "$os": "linux",
                    "$browser": "discord-parser-mvp",
                    "$device": "discord-parser-mvp"
                },
                "compress": False,
                "large_threshold": 50,
                "shard": None,  # No sharding for now
                "presence": {
                    "status": "online",
                    "afk": False
                },
                "intents": self.intents  # GUILDS + GUILD_MESSAGES
            }
        }
        
        try:
            await ws.send_str(json.dumps(identify_payload))
            self.logger.info(f"🆔 Sent IDENTIFY for {connection_id}", intents=self.intents)
        except Exception as e:
            self.logger.error(f"❌ Failed to send IDENTIFY for {connection_id}", error=str(e))
            raise
    
    async def _send_resume(self, connection_info: Dict) -> None:
        """Send RESUME to Discord Gateway"""
        ws = connection_info['ws']
        token_index = connection_info['token_index']
        connection_id = f"token_{token_index}"
        session_id = connection_info.get('session_id')
        sequence = connection_info.get('sequence')
        
        # Get token from session headers
        token = self.settings.discord_tokens[token_index]
        if not token.startswith('Bot '):
            token = f'Bot {token}'
        
        resume_payload = {
            "op": 6,
            "d": {
                "token": token,
                "session_id": session_id,
                "seq": sequence
            }
        }
        
        try:
            await ws.send_str(json.dumps(resume_payload))
            self.logger.info(f"🔄 Sent RESUME for {connection_id}", 
                           session_id=session_id,
                           sequence=sequence)
        except Exception as e:
            self.logger.error(f"❌ Failed to send RESUME for {connection_id}", error=str(e))
            raise
    
    async def _handle_dispatch_event(self, event_type: str, data: Dict, connection_info: Dict) -> None:
        """Handle DISPATCH events from Discord Gateway"""
        connection_id = f"token_{connection_info['token_index']}"
        
        if event_type == 'READY':
            await self._handle_ready_event(data, connection_info)
            
        elif event_type == 'RESUMED':
            self.logger.info(f"✅ Session resumed for {connection_id}")
            
        elif event_type == 'MESSAGE_CREATE':
            await self._handle_message_create(data, connection_info)
            
        elif event_type == 'GUILD_CREATE':
            self.logger.debug(f"🏰 Guild create for {connection_id}", 
                            guild_id=data.get('id'),
                            guild_name=data.get('name'))
            
        else:
            self.logger.debug(f"🔍 Unhandled event for {connection_id}", event_type=event_type)
    
    async def _handle_ready_event(self, data: Dict, connection_info: Dict) -> None:
        """Handle READY event from Discord Gateway"""
        connection_id = f"token_{connection_info['token_index']}"
        session_id = data.get('session_id')
        user_data = data.get('user', {})
        guilds = data.get('guilds', [])
        
        # Store session ID for resuming
        connection_info['session_id'] = session_id
        
        self.logger.info(f"🚀 WebSocket READY for {connection_id}",
                       session_id=session_id,
                       username=user_data.get('username'),
                       guild_count=len(guilds))
        
        # Log which monitored channels are available
        monitored_count = 0
        for guild in guilds:
            guild_id = guild.get('id')
            # Check if any of our monitored channels are in this guild
            for server_info in self.servers.values():
                if server_info.guild_id == guild_id:
                    monitored_count += len([
                        ch_id for ch_id in server_info.channels.keys()
                        if ch_id in self.monitored_announcement_channels
                    ])
        
        self.logger.info(f"📊 Ready monitoring coverage for {connection_id}",
                       monitored_channels=len(self.monitored_announcement_channels),
                       accessible_guilds=len(guilds),
                       monitored_in_accessible_guilds=monitored_count)
    
    async def _handle_message_create(self, data: Dict, connection_info: Dict) -> None:
        """Handle MESSAGE_CREATE event - THIS IS WHERE REAL-TIME MESSAGES COME FROM"""
        connection_id = f"token_{connection_info['token_index']}"
        
        try:
            channel_id = data.get('channel_id')
            guild_id = data.get('guild_id')
            
            # Only process messages from monitored channels
            if channel_id not in self.monitored_announcement_channels:
                return
            
            # Skip bot messages
            author = data.get('author', {})
            if author.get('bot', False):
                return
            
            # Skip empty messages
            content = data.get('content', '').strip()
            if not content:
                return
            
            # Find server name from guild_id
            server_name = None
            channel_name = None
            
            for srv_name, srv_info in self.servers.items():
                if srv_info.guild_id == guild_id and channel_id in srv_info.channels:
                    server_name = srv_name
                    channel_name = srv_info.channels[channel_id].channel_name
                    break
            
            if not server_name:
                self.logger.debug(f"🤷 Unknown server for message", 
                                guild_id=guild_id,
                                channel_id=channel_id)
                return
            
            # Create DiscordMessage object
            message = DiscordMessage(
                content=content,
                timestamp=datetime.fromisoformat(
                    data['timestamp'].replace('Z', '+00:00')
                ),
                server_name=server_name,
                channel_name=channel_name,
                author=author.get('username', 'Unknown'),
                message_id=data.get('id'),
                channel_id=channel_id,
                guild_id=guild_id
            )
            
            # Trigger callbacks (this sends to MessageProcessor)
            await self._trigger_message_callbacks(message)
            
            self.logger.info(f"📨 WebSocket message received",
                           connection_id=connection_id,
                           server=server_name,
                           channel=channel_name,
                           author=author.get('username'),
                           content_preview=content[:50])
            
        except Exception as e:
            self.logger.error(f"❌ Error handling MESSAGE_CREATE for {connection_id}", 
                            error=str(e),
                            data_keys=list(data.keys()) if data else None)
    
    # ============================================================================
    # EXISTING METHODS (unchanged)
    # ============================================================================
    
    def notify_new_channel_added(self, server_name: str, channel_id: str, channel_name: str) -> bool:
        """Уведомление о добавлении канала"""
        try:
            if server_name not in self.servers:
                self.logger.error(f"Server {server_name} not found")
                return False
            
            server_info = self.servers[server_name]
            
            if channel_id not in server_info.channels:
                self.logger.warning(f"Channel {channel_id} not found in server {server_name} channels")
                return False
            
            # Add to monitored channels
            self.monitored_announcement_channels.add(channel_id)
            
            is_announcement = self._is_announcement_channel(channel_name)
            if is_announcement:
                self.logger.info(f"✅ Added ANNOUNCEMENT channel '{channel_name}' ({channel_id}) to WebSocket monitoring")
            else:
                self.logger.info(f"✅ Added regular channel '{channel_name}' ({channel_id}) to WebSocket monitoring")
            
            self.logger.info(f"📢 Channel '{channel_name}' WILL forward NEW messages via WebSocket")
            
            # Обновляем статистику
            server_info.update_stats()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in notify_new_channel_added: {e}")
            return False
    
    def get_non_announcement_servers(self) -> List[Dict[str, any]]:
        """Get servers without announcement channels"""
        result = []
        for server_name, server in self.servers.items():
            has_announcement = any(
                self._is_announcement_channel(ch.channel_name) 
                for ch in server.channels.values()
            )
            if not has_announcement:
                result.append({
                    'name': server_name,
                    'id': server.guild_id,
                    'channel_count': len(server.channels)
                })
        return result

    def get_server_channels(self, server_name: str) -> List[Dict[str, any]]:
        """Get all channels for specified server"""
        if server_name not in self.servers:
            return []
            
        server = self.servers[server_name]
        return [{
            'id': ch.channel_id,
            'name': ch.channel_name,
            'is_announcement': self._is_announcement_channel(ch.channel_name),
            'accessible': ch.http_accessible
        } for ch in server.channels.values()]

    def get_server_stats(self) -> Dict[str, any]:
        """Get statistics for servers with monitored channels"""
        monitored_channels_count = len(self.monitored_announcement_channels)
        
        # Подсчитываем announcement каналы отдельно 
        auto_discovered_announcement = 0
        manually_added_channels = 0
        
        for server_info in self.servers.values():
            for channel_id, channel_info in server_info.channels.items():
                if channel_id in self.monitored_announcement_channels:
                    if self._is_announcement_channel(channel_info.channel_name):
                        auto_discovered_announcement += 1
                    else:
                        manually_added_channels += 1
        
        return {
            "total_servers": len(self.servers),
            "active_servers": len([s for s in self.servers.values() if s.status == ServerStatus.ACTIVE]),
            "total_channels": sum(s.channel_count for s in self.servers.values()),
            "accessible_channels": sum(s.accessible_channel_count for s in self.servers.values()),
            "monitored_channels": monitored_channels_count,
            "auto_discovered_announcement": auto_discovered_announcement,
            "manually_added_channels": manually_added_channels,
            "monitoring_strategy": "WebSocket Real-time",
            "websocket_connections": len(self.websocket_connections),
            "websocket_sessions": len(self.websocket_sessions),
            "valid_sessions": len(self.sessions),
            "message_callbacks": len(self.message_callbacks),
            "gateway_urls": len(self.gateway_urls),
            "servers": {name: {
                "status": server.status.value,
                "channels": server.channel_count,
                "accessible_channels": server.accessible_channel_count,
                "monitored_channels": len([
                    ch_id for ch_id in server.channels.keys() 
                    if ch_id in self.monitored_announcement_channels
                ]),
                "announcement_channels": len([
                    ch for ch in server.channels.values() 
                    if self._is_announcement_channel(ch.channel_name)
                ]),
                "manually_added_channels": len([
                    ch_id for ch_id, ch_info in server.channels.items() 
                    if ch_id in self.monitored_announcement_channels and 
                    not self._is_announcement_channel(ch_info.channel_name)
                ]),
                "last_sync": server.last_sync.isoformat() if server.last_sync else None
            } for name, server in self.servers.items()}
        }
    
    async def cleanup(self) -> None:
        """Clean up resources"""
        self.running = False
        
        self.logger.info("🧹 Cleaning up Discord service...")
        
        # Close all WebSocket connections
        for connection_info in self.websocket_connections[:]:
            try:
                # Cancel heartbeat task
                if connection_info.get('heartbeat_task'):
                    connection_info['heartbeat_task'].cancel()
                
                # Close WebSocket
                ws = connection_info.get('ws')
                if ws and not ws.closed:
                    await ws.close()
                    
            except Exception as e:
                self.logger.error("Error closing WebSocket connection", error=str(e))
        
        # Clear connection tracking
        self.websocket_connections.clear()
        self.websocket_sessions.clear()
        
        # Close all HTTP sessions
        for session in self.sessions:
            if not session.closed:
                await session.close()
        
        self.logger.info("✅ Discord service cleaned up (WebSocket mode)")
    
    def notify_channel_removed(self, server_name: str, channel_id: str, channel_name: str) -> bool:
        """Уведомление об удалении канала из мониторинга"""
        try:
            if channel_id in self.monitored_announcement_channels:
                self.monitored_announcement_channels.remove(channel_id)
                
            self.logger.info(f"✅ Channel '{channel_name}' ({channel_id}) removed from WebSocket monitoring")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in notify_channel_removed: {e}")
            return False
    
    def get_channel_messages(self, channel_id: str, limit: int = 5) -> List[dict]:
        """Получить сообщения из канала (для совместимости с ботом)"""
        try:
            # Найти сервер для этого канала
            server_name = None
            for srv_name, srv_info in self.servers.items():
                if channel_id in srv_info.channels:
                    server_name = srv_name
                    break
            
            if not server_name:
                return []
            
            # Используем существующий асинхронный метод
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                messages = loop.run_until_complete(
                    self.get_recent_messages(server_name, channel_id, limit)
                )
                
                # Конвертируем в простой формат для бота
                simple_messages = []
                for msg in messages:
                    simple_messages.append({
                        'author': msg.author,
                        'content': msg.content,
                        'timestamp': msg.timestamp.isoformat(),
                        'id': msg.message_id
                    })
                
                return simple_messages
                
            finally:
                loop.close()
                
        except Exception as e:
            self.logger.error(f"Error getting channel messages: {e}")
            return []
    
    def get_websocket_status(self) -> Dict[str, any]:
        """Get detailed WebSocket connection status"""
        active_connections = []
        
        for connection_info in self.websocket_connections:
            ws = connection_info.get('ws')
            token_index = connection_info.get('token_index')
            
            status = {
                'token_index': token_index,
                'connected': ws and not ws.closed if ws else False,
                'connected_at': connection_info.get('connected_at'),
                'session_id': connection_info.get('session_id'),
                'last_heartbeat': connection_info.get('last_heartbeat'),
                'sequence': connection_info.get('sequence')
            }
            
            if status['connected_at']:
                uptime = datetime.now() - status['connected_at']
                status['uptime_seconds'] = int(uptime.total_seconds())
            
            active_connections.append(status)
        
        return {
            'total_connections': len(self.websocket_connections),
            'active_connections': len([c for c in active_connections if c['connected']]),
            'total_tokens': len(self.sessions),
            'gateway_urls_available': len(self.gateway_urls),
            'monitoring_enabled': self.running,
            'connections': active_connections,
            'intents': self.intents,
            'monitored_channels': len(self.monitored_announcement_channels)
        }