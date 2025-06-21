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
        
        self.extended_channel_keywords = [
            'announcement', 'announcements', 'announce',
            'news', 'updates', 'update', 'info', 'information',
            'general', 'main', 'important', 'notice', 'notices',
            'alert', 'alerts', 'feed', 'channel', 'official'
        ]
    
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
        """РАСШИРЕННАЯ проверка каналов - для бота (показ всех подходящих)"""
        # Удаляем emoji и лишние пробелы из названия
        clean_name = ''.join([c for c in channel_name if c.isalpha() or c.isspace()])
        clean_name = ' '.join(clean_name.split()).lower()
        
        # Проверяем содержит ли очищенное название любое из РАСШИРЕННЫХ ключевых слов
        for keyword in self.extended_channel_keywords:
            if keyword in clean_name:
                return True
        return False
    def _is_strict_announcement_channel(self, channel_name: str) -> bool:
        """СТРОГАЯ проверка ТОЛЬКО announcement каналов - для автоматического добавления"""
        # Удаляем emoji и лишние пробелы из названия
        clean_name = ''.join([c for c in channel_name if c.isalpha() or c.isspace()])
        clean_name = ' '.join(clean_name.split()).lower()
        
        # Проверяем ТОЛЬКО announcement ключевые слова
        announcement_keywords = ['announcement', 'announcements', 'announce']
        for keyword in announcement_keywords:
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
        """ИСПРАВЛЕНО: Получить ВСЕ серверы и добавить их в топики (сохраняем название метода)"""
        if not self.sessions:
            return
        
        self.logger.info("🔍 Discovering ALL servers and creating topics for each...")
        
        # Получаем ВСЕ гильдии со всех токенов
        all_guilds = await self._fetch_all_guilds_from_all_tokens()
        
        if not all_guilds:
            self.logger.error("No guilds found from any token")
            return
        
        self.logger.info(f"📊 Found {len(all_guilds)} total servers across all tokens")
        
        # ИСПРАВЛЕНО: Берем ВСЕ серверы, не ограничиваем по max_servers искусственно
        actual_max_servers = min(len(all_guilds), max(self.settings.max_servers, len(all_guilds)))
        guilds_to_process = all_guilds[:actual_max_servers]
        
        self.logger.info(f"🎯 Processing ALL {len(guilds_to_process)} servers")
        
        # Обрабатываем серверы батчами для лучшей производительности
        batch_size = self.settings.server_discovery_batch_size
        total_batches = (len(guilds_to_process) + batch_size - 1) // batch_size
        
        processed_count = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(guilds_to_process))
            batch = guilds_to_process[start_idx:end_idx]
            
            self.logger.info(f"📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch)} servers)")
            
            # Создаем задачи для обработки серверов в батче
            batch_tasks = []
            for guild in batch:
                task = self._process_guild_announcement_channels_safe(guild)
                batch_tasks.append(task)
            
            # Выполняем батч с таймаутом
            try:
                batch_results = await asyncio.wait_for(
                    asyncio.gather(*batch_tasks, return_exceptions=True),
                    timeout=120  # 2 минуты на батч
                )
                
                # Подсчитываем результаты
                for i, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        guild_name = batch[i].get('name', 'Unknown')
                        self.logger.error(f"❌ Failed to process server {guild_name}: {result}")
                    elif result:
                        processed_count += 1
                        
            except asyncio.TimeoutError:
                self.logger.error(f"❌ Batch {batch_num + 1} timed out")
            
            # Пауза между батчами
            if batch_num < total_batches - 1:
                await asyncio.sleep(1)
    
    async def _process_guild_announcement_channels_only(self, session: aiohttp.ClientSession, guild_data: dict) -> None:
        """ИСПРАВЛЕНО: Process guild - ВСЕ серверы сохраняются, ищем подходящие каналы"""
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
                            # Сервер недоступен - пропускаем полностью
                            self.logger.warning(f"⚠️ No access to server '{guild_name}' (HTTP {response.status}), skipping")
                            return
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.base_delay * (2 ** attempt))
                            continue
                        
                        # При ошибке - пропускаем сервер
                        self.logger.warning(f"⚠️ Error accessing server '{guild_name}' (HTTP {response.status}), skipping")
                        return
                    
                    channels = await response.json()
                    
                    # Create server info
                    server_info = ServerInfo(
                        server_name=guild_name,
                        guild_id=guild_id,
                        max_channels=self.settings.max_channels_per_server
                    )
                    
                    # Ищем ТОЛЬКО announcement каналы для автоматического добавления
                    announcement_channels = self._find_announcement_channels_only(channels)
                    
                    if announcement_channels:
                        self.logger.info(f"✅ Found {len(announcement_channels)} announcement channels in '{guild_name}'")
                        
                        # Add ТОЛЬКО announcement каналы к серверу
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
                                # Инициализируем отслеживание для polling
                                self.last_seen_message_per_channel[channel['id']] = None
                                self.channel_last_poll_time[channel['id']] = datetime.now()
                        
                        # Сохраняем сервер ТОЛЬКО если есть announcement каналы
                        server_info.update_stats()
                        self.servers[guild_name] = server_info
                        
                        self.logger.info("✅ Server with announcement channels added", 
                                    guild=guild_name,
                                    announcement_channels=len(announcement_channels),
                                    accessible_channels=server_info.accessible_channel_count)
                    else:
                        self.logger.info(f"📭 No announcement channels found in '{guild_name}', server skipped")
                        return
                    
                    self.logger.info("✅ Server processed and will get Telegram topic", 
                                guild=guild_name,
                                total_channels=len(server_info.channels),
                                accessible_channels=server_info.accessible_channel_count,
                                monitored_channels=len([ch_id for ch_id in server_info.channels.keys() 
                                                        if ch_id in self.monitored_announcement_channels]))
                    
                    return True
                    
            except Exception as e:
                self.logger.error("Error processing guild", 
                                guild=guild_name, 
                                error=str(e),
                                attempt=attempt + 1)
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.base_delay * (2 ** attempt))
        
        # Если не удалось обработать - пропускаем сервер
        self.logger.warning(f"⚠️ Failed to process '{guild_name}', server skipped")
    
    
    
    def _find_announcement_channels_only(self, channels: List[dict]) -> List[dict]:
        """Найти ТОЛЬКО announcement каналы (строгая фильтрация)"""
        announcement_channels = []
        
        for channel in channels:
            if channel.get('type') not in [0, 5]:  # Text channels and announcement channels
                continue
            
            channel_name = channel['name']
            
            # Проверяем ТОЛЬКО по announcement ключевым словам
            if self._is_strict_announcement_channel(channel_name):
                announcement_channels.append(channel)
                self.logger.info(
                    "Found announcement channel", 
                    original_name=channel_name,
                    channel_id=channel['id']
                )
        
        self.logger.info(f"Total announcement channels found: {len(announcement_channels)}")
        return announcement_channels
    
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
    
    async def _fetch_guilds_from_single_token(self, session: aiohttp.ClientSession, token_index: int) -> List[dict]:
        """Получить гильдии с одного токена"""
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"guilds_token_{token_index}")
                
                async with session.get('https://discord.com/api/v9/users/@me/guilds') as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 30))
                        self.logger.warning(f"⏳ Rate limited on token {token_index}, waiting {retry_after}s")
                        await asyncio.sleep(min(retry_after, 60))
                        continue
                    
                    if response.status != 200:
                        if response.status in [401, 403]:
                            self.logger.error(f"❌ Token {token_index} unauthorized (HTTP {response.status})")
                            break
                        
                        self.logger.warning(f"⚠️ Token {token_index} HTTP {response.status}, attempt {attempt + 1}")
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.base_delay * (2 ** attempt))
                            continue
                        break
                    
                    guilds = await response.json()
                    self.logger.info(f"✅ Token {token_index}: {len(guilds)} servers found")
                    return guilds
                    
            except Exception as e:
                self.logger.error(f"❌ Error fetching guilds from token {token_index}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.base_delay * (2 ** attempt))
        
        self.logger.error(f"❌ Failed to fetch guilds from token {token_index} after {self.max_retries} attempts")
        return []
    
    async def _process_guild_announcement_channels_safe(self, guild_data: dict) -> bool:
        """Безопасная обработка гильдии с защитой от ошибок"""
        guild_name = guild_data.get('name', 'Unknown')
        guild_id = guild_data.get('id')
        source_token = guild_data.get('_source_token', 0)
        
        try:
            # Используем токен, который нашел эту гильдию, или случайный
            if source_token < len(self.sessions):
                session = self.sessions[source_token]
            else:
                session = self._get_healthy_session()
            
            if not session:
                self.logger.error(f"❌ No healthy session for guild {guild_name}")
                return False
            
            # Применяем таймаут для обработки гильдии
            timeout = getattr(self.settings, 'channel_test_timeout', 10) * 2
            
            return await asyncio.wait_for(
                self._process_guild_announcement_channels_only(session, guild_data),
                timeout=timeout
            )
            
        except asyncio.TimeoutError:
            self.logger.error(f"⏰ Guild {guild_name} processing timed out after {timeout}s")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error processing guild {guild_name}: {e}")
            return False
    
    
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
    
    def get_channel_categories(self, channel_name: str) -> List[str]:
        """НОВОЕ: Определить категории канала"""
        clean_name = ''.join([c for c in channel_name if c.isalpha() or c.isspace()])
        clean_name = ' '.join(clean_name.split()).lower()
        
        categories = []
        
        # Announcement categories
        if any(word in clean_name for word in ['announcement', 'announcements', 'announce']):
            categories.append('📢 Announcements')
        
        # News categories  
        if any(word in clean_name for word in ['news', 'updates', 'update']):
            categories.append('📰 News & Updates')
        
        # Info categories
        if any(word in clean_name for word in ['info', 'information', 'guide', 'help']):
            categories.append('ℹ️ Information')
        
        # General categories
        if any(word in clean_name for word in ['general', 'main', 'chat', 'discussion']):
            categories.append('💬 General')
        
        # Important categories
        if any(word in clean_name for word in ['important', 'notice', 'alert', 'official']):
            categories.append('⚠️ Important')
        
        if not categories:
            categories.append('📝 Other')
        
        return categories
    
    def get_server_channels_by_category(self, server_name: str) -> Dict[str, List[dict]]:
        """НОВОЕ: Получить каналы сервера, сгруппированные по категориям"""
        if server_name not in self.servers:
            return {}
        
        server_info = self.servers[server_name]
        channels_by_category = {}
        
        for channel_id, channel_info in server_info.channels.items():
            categories = self.get_channel_categories(channel_info.channel_name)
            is_monitored = channel_id in self.monitored_announcement_channels
            
            channel_data = {
                'channel_id': channel_id,
                'channel_name': channel_info.channel_name,
                'accessible': channel_info.http_accessible,
                'monitored': is_monitored,
                'message_count': getattr(channel_info, 'message_count', 0)
            }
            
            for category in categories:
                if category not in channels_by_category:
                    channels_by_category[category] = []
                channels_by_category[category].append(channel_data)
        
        return channels_by_category
    
    async def discover_channels_for_server(self, server_name: str) -> Dict[str, List[dict]]:
        """НОВОЕ: Заново просканировать каналы конкретного сервера"""
        if server_name not in self.servers:
            return {}
        
        server_info = self.servers[server_name]
        guild_id = server_info.guild_id
        
        session = self._get_healthy_session()
        if not session:
            return {}
        
        try:
            await self.rate_limiter.wait_if_needed(f"rediscover_{guild_id}")
            
            async with session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels') as response:
                if response.status != 200:
                    self.logger.error(f"Failed to rediscover channels for {server_name}: HTTP {response.status}")
                    return {}
                
                channels = await response.json()
                
                # Анализируем ВСЕ текстовые каналы
                all_channels_by_category = {}
                
                for channel in channels:
                    if channel.get('type') not in [0, 5]:  # Only text channels
                        continue
                    
                    channel_name = channel['name']
                    categories = self.get_channel_categories(channel_name)
                    
                    # Проверяем доступность канала
                    is_accessible = await self._test_channel_access_with_retry(session, channel['id'])
                    is_monitored = channel['id'] in self.monitored_announcement_channels
                    
                    channel_data = {
                        'channel_id': channel['id'],
                        'channel_name': channel_name,
                        'accessible': is_accessible,
                        'monitored': is_monitored,
                        'can_add': is_accessible and not is_monitored,
                        'message_count': 0
                    }
                    
                    for category in categories:
                        if category not in all_channels_by_category:
                            all_channels_by_category[category] = []
                        all_channels_by_category[category].append(channel_data)
                
                self.logger.info(f"Rediscovered channels for {server_name}", 
                            total_channels=len(channels),
                            categories=len(all_channels_by_category))
                
                return all_channels_by_category
                
        except Exception as e:
            self.logger.error(f"Error rediscovering channels for {server_name}: {e}")
            return {}
        
    
    
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