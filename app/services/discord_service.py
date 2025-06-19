# app/services/discord_service.py - ИСПРАВЛЕННАЯ ВЕРСИЯ без дублирования в polling
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
    """Discord service - БЕЗ дублирования сообщений в polling"""
    
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
        self.websocket_connections: List[aiohttp.ClientWebSocketResponse] = []
        
        # Channel monitoring
        self.message_callbacks: List[Callable] = []
        self.monitored_announcement_channels: Set[str] = set()
        
        # НОВОЕ: Отслеживание последних сообщений для polling
        self.last_seen_message_per_channel: Dict[str, str] = {}  # channel_id -> last_message_id
        self.channel_last_poll_time: Dict[str, datetime] = {}  # channel_id -> last_poll_time
        
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
    
    def _is_announcement_channel(self, channel_name: str) -> bool:
        """Проверка что канал является announcement"""
        # Удаляем emoji и лишние пробелы из названия
        clean_name = ''.join([c for c in channel_name if c.isalpha() or c.isspace()])
        clean_name = ' '.join(clean_name.split()).lower()
        
        # Проверяем содержит ли очищенное название любое из ключевых слов
        for keyword in self.settings.channel_keywords:
            if keyword in clean_name:
                return True
        return False
    
    async def initialize(self) -> bool:
        """Initialize Discord service"""
        if self._initialization_done:
            return True
            
        self.logger.info("Initializing Discord service with anti-duplication polling", 
                        token_count=len(self.settings.discord_tokens),
                        max_servers=self.settings.max_servers,
                        max_channels_total=self.settings.max_total_channels)
        
        # Create sessions
        successful_tokens = 0
        for i, token in enumerate(self.settings.discord_tokens):
            session = aiohttp.ClientSession(
                headers={
                    'Authorization': token,
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
            
            if await self._validate_token_with_retry(session, i):
                self.sessions.append(session)
                self.token_failure_counts[i] = 0
                successful_tokens += 1
                self.logger.info("Token validated successfully", token_index=i)
            else:
                await session.close()
                self.logger.error("Token validation failed permanently", token_index=i)
        
        if not self.sessions:
            self.logger.error("No valid Discord tokens available")
            return False
        
        # Discover announcement channels
        await self._discover_announcement_channels_only()
        
        self._initialization_done = True
        self.logger.info("Discord service initialized with anti-duplication", 
                        valid_tokens=len(self.sessions),
                        servers_found=len(self.servers),
                        announcement_channels=len(self.monitored_announcement_channels))
        return True
    
    async def _validate_token_with_retry(self, session: aiohttp.ClientSession, token_index: int) -> bool:
        """Validate token with retry logic"""
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    delay = min(self.max_delay, self.base_delay * (2 ** attempt))
                    await asyncio.sleep(delay)
                
                await self.rate_limiter.wait_if_needed(f"token_validate_{token_index}")
                
                async with session.get('https://discord.com/api/v9/users/@me') as response:
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
                
                # Test guild access
                async with session.get('https://discord.com/api/v9/users/@me/guilds') as guilds_res:
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
                
                async with session.get('https://discord.com/api/v9/users/@me/guilds') as response:
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
                
                async with session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels') as response:
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
                            # НОВОЕ: Инициализируем отслеживание для polling
                            self.last_seen_message_per_channel[channel['id']] = None
                            self.channel_last_poll_time[channel['id']] = datetime.now()
                    
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
        """Find announcement channels"""
        announcement_channels = []
        
        for channel in channels:
            if channel.get('type') not in [0, 5]:  # Text channels and announcement channels
                continue
                
            if self._is_announcement_channel(channel['name']):
                announcement_channels.append(channel)
                self.logger.info(
                    "Found announcement channel", 
                    original_name=channel['name'],
                    channel_id=channel['id']
                )
        
        self.logger.info("Total announcement channels found", count=len(announcement_channels))
        return announcement_channels
    
    async def _test_channel_access_with_retry(self, session: aiohttp.ClientSession, channel_id: str) -> bool:
        """Test channel access with retry logic"""
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"test_channel_{channel_id}")
                
                async with session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1') as response:
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
                             limit: int = 5) -> List[DiscordMessage]:
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
        actual_limit = min(limit, 20)  # Increased limit for better message retrieval
        
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"messages_{channel_id}")
                
                async with session.get(
                    f'https://discord.com/api/v9/channels/{channel_id}/messages',
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
    
    async def get_new_messages_only(self, 
                                  server_name: str, 
                                  channel_id: str, 
                                  limit: int = 10) -> List[DiscordMessage]:
        """НОВОЕ: Get only NEW messages since last poll (для polling без дублирования)"""
        if server_name not in self.servers:
            return []

        server = self.servers[server_name]
        if channel_id not in server.channels:
            return []

        channel = server.channels[channel_id]
        
        if channel_id not in self.monitored_announcement_channels:
            return []

        if not channel.http_accessible:
            return []

        session = self._get_healthy_session()
        if not session:
            return []

        # Получаем последний известный message_id для этого канала
        last_seen_message_id = self.last_seen_message_per_channel.get(channel_id)
        
        messages = []
        actual_limit = min(limit, 20)
        
        try:
            await self.rate_limiter.wait_if_needed(f"new_messages_{channel_id}")
            
            # Строим URL для получения сообщений
            url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
            params = {'limit': actual_limit}
            
            # Если есть последнее сообщение, получаем только сообщения после него
            if last_seen_message_id:
                params['after'] = last_seen_message_id
            
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    self.logger.warning("Failed to fetch new messages", 
                                      channel_id=channel_id,
                                      status=response.status)
                    return []
                
                raw_messages = await response.json()
                self.rate_limiter.record_success()
                
                # Если нет новых сообщений
                if not raw_messages:
                    self.logger.debug("No new messages found", 
                                    channel_id=channel_id,
                                    last_seen=last_seen_message_id)
                    return []
                
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
                        self.logger.warning("Failed to parse new message", 
                                          message_id=raw_msg.get('id'),
                                          error=str(e))
                        continue
                
                # Обновляем последний известный message_id
                if messages:
                    # Сортируем по timestamp и берем самое новое
                    latest_message = max(messages, key=lambda x: x.timestamp)
                    self.last_seen_message_per_channel[channel_id] = latest_message.message_id
                    
                    self.logger.info("Found NEW messages in polling", 
                                   channel_id=channel_id,
                                   channel_name=channel.channel_name,
                                   new_message_count=len(messages),
                                   latest_message_id=latest_message.message_id)
                
                # Обновляем время последнего polling
                self.channel_last_poll_time[channel_id] = datetime.now()
                
                return sorted(messages, key=lambda x: x.timestamp)
                
        except Exception as e:
            self.logger.error("Error getting new messages", 
                            server=server_name,
                            channel_id=channel_id,
                            error=str(e))
            self.rate_limiter.record_error()
        
        return []
    
    def set_telegram_service_ref(self, telegram_service):
        """Set reference to Telegram service for integration"""
        self.telegram_service_ref = telegram_service
        self.logger.info("Telegram service reference set for Discord integration")
    
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
    
    async def _http_polling_loop_new_messages_only(self) -> None:
        """ИСПРАВЛЕНО: HTTP polling - ТОЛЬКО новые сообщения"""
        base_poll_interval = 30  # Poll every 30 seconds for new messages
        error_count = 0
        
        while self.running:
            try:
                poll_start = datetime.now()
                
                # Группируем каналы по серверам для эффективности
                server_channel_map = {}
                for channel_id in self.monitored_announcement_channels:
                    # Найти сервер для этого канала
                    server_name = None
                    for srv_name, srv_info in self.servers.items():
                        if srv_info.status != ServerStatus.ACTIVE:
                            continue
                        if channel_id in srv_info.channels:
                            server_name = srv_name
                            break
                    
                    if server_name:
                        if server_name not in server_channel_map:
                            server_channel_map[server_name] = []
                        server_channel_map[server_name].append(channel_id)
                
                # Создаем задачи для polling ТОЛЬКО новых сообщений
                tasks = []
                for server_name, channel_ids in server_channel_map.items():
                    for channel_id in channel_ids:
                        task = self._poll_channel_for_new_messages_only(server_name, channel_id)
                        tasks.append(task)
                
                if tasks:
                    semaphore = asyncio.Semaphore(3)
                    
                    async def poll_with_semaphore(task):
                        async with semaphore:
                            return await task
                    
                    results = await asyncio.gather(
                        *[poll_with_semaphore(task) for task in tasks],
                        return_exceptions=True
                    )
                    
                    successful_polls = sum(1 for result in results if result and not isinstance(result, Exception))
                    new_messages_found = sum(result if isinstance(result, int) and result > 0 else 0 for result in results)
                    
                    # Подсчет типов каналов для статистики
                    announcement_polls = 0
                    regular_polls = 0
                    for server_name, channel_ids in server_channel_map.items():
                        for channel_id in channel_ids:
                            if server_name in self.servers and channel_id in self.servers[server_name].channels:
                                channel_info = self.servers[server_name].channels[channel_id]
                                if self._is_announcement_channel(channel_info.channel_name):
                                    announcement_polls += 1
                                else:
                                    regular_polls += 1
                    
                    if new_messages_found > 0:
                        self.logger.info("New messages polling cycle completed", 
                                    total_polls=len(tasks),
                                    successful_polls=successful_polls,
                                    new_messages_found=new_messages_found,
                                    announcement_channels=announcement_polls,
                                    regular_channels=regular_polls,
                                    duration_seconds=(datetime.now() - poll_start).total_seconds())
                    else:
                        self.logger.debug("Polling cycle completed - no new messages", 
                                    total_polls=len(tasks),
                                    successful_polls=successful_polls)
                    
                    error_count = 0
                else:
                    self.logger.debug("No monitored channels to poll")
                
                # Adaptive polling interval
                poll_interval = base_poll_interval
                if error_count > 3:
                    poll_interval = min(300, base_poll_interval * (2 ** min(error_count - 3, 3)))
                
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                error_count += 1
                self.logger.error("Error in new messages polling loop", 
                                error=str(e),
                                error_count=error_count)
                
                error_delay = min(300, 30 * (2 ** min(error_count, 4)))
                await asyncio.sleep(error_delay)
    
    async def _poll_channel_for_new_messages_only(self, server_name: str, channel_id: str) -> int:
        """ИСПРАВЛЕНО: Poll канал ТОЛЬКО для новых сообщений"""
        try:
            # Получаем ТОЛЬКО новые сообщения
            new_messages = await self.get_new_messages_only(server_name, channel_id, limit=10)
            
            if new_messages:
                channel_info = self.servers[server_name].channels[channel_id]
                channel_type = "announcement" if self._is_announcement_channel(channel_info.channel_name) else "regular"
                
                self.logger.info("Found NEW messages during polling", 
                                server=server_name,
                                channel_name=channel_info.channel_name,
                                channel_type=channel_type,
                                channel_id=channel_id,
                                new_message_count=len(new_messages))
                
                # Trigger callbacks for each NEW message
                for message in new_messages:
                    await self._trigger_message_callbacks(message)
                
                return len(new_messages)
            else:
                # Нет новых сообщений - это нормально
                return 0
            
        except Exception as e:
            self.logger.error("Error polling channel for new messages", 
                            server=server_name,
                            channel_id=channel_id,
                            error=str(e))
            return -1  # Ошибка
    
    async def start_websocket_monitoring(self) -> None:
        """ИСПРАВЛЕНО: Start HTTP polling для ТОЛЬКО новых сообщений"""
        self.logger.info("Starting HTTP polling for NEW messages only", 
                    monitored_channels=len(self.monitored_announcement_channels),
                    strategy="Poll for new messages only - no duplicates")
        
        if not self.sessions:
            self.logger.error("No valid sessions for monitoring")
            return
        
        self.running = True
        
        try:
            await self._http_polling_loop_new_messages_only()
        except Exception as e:
            self.logger.error("HTTP polling monitoring failed", error=str(e))
        finally:
            self.running = False
            
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
            
            # Добавляем в monitored channels
            self.monitored_announcement_channels.add(channel_id)
            
            # НОВОЕ: Инициализируем отслеживание для polling
            self.last_seen_message_per_channel[channel_id] = None
            self.channel_last_poll_time[channel_id] = datetime.now()
            
            is_announcement = self._is_announcement_channel(channel_name)
            if is_announcement:
                self.logger.info(f"✅ Added ANNOUNCEMENT channel '{channel_name}' ({channel_id}) to monitoring")
            else:
                self.logger.info(f"✅ Added regular channel '{channel_name}' ({channel_id}) to monitoring")
            
            self.logger.info(f"📢 Channel '{channel_name}' WILL forward NEW messages to Telegram")
            self.logger.info(f"🔔 Manual addition = automatic monitoring with anti-duplication")
            
            # Обновляем статистику
            server_info.update_stats()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in notify_new_channel_added: {e}")
            return False
    
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
            "monitoring_strategy": "auto announcement + manual any",
            "polling_strategy": "new messages only - no duplicates",  # НОВОЕ
            "valid_sessions": len(self.sessions),
            "message_callbacks": len(self.message_callbacks),
            "channels_with_tracking": len(self.last_seen_message_per_channel),  # НОВОЕ
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
        
        # Close all WebSocket connections
        for ws in self.websocket_connections:
            if not ws.closed:
                await ws.close()
        
        # Close all HTTP sessions
        for session in self.sessions:
            if not session.closed:
                await session.close()
        
        self.logger.info("Discord service cleaned up (new messages only polling)")