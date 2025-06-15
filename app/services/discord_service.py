# app/services/discord_service.py - IMPROVED VERSION
import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
import structlog
import random

from ..models.message import DiscordMessage
from ..models.server import ServerInfo, ChannelInfo, ServerStatus
from ..config import Settings
from ..utils.rate_limiter import RateLimiter

class DiscordService:
    """Improved Discord service with better error handling and rate limiting"""
    
    def __init__(self, 
                 settings: Settings,
                 rate_limiter: RateLimiter,
                 redis_client = None,
                 logger = None):
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.redis_client = redis_client
        self.logger = logger or structlog.get_logger(__name__)
        
        # Session management with better error handling
        self.sessions: List[aiohttp.ClientSession] = []
        self.current_token_index = 0
        self.token_failure_counts: Dict[int, int] = {}
        
        # Server tracking
        self.servers: Dict[str, ServerInfo] = {}
        self.websocket_connections: List[aiohttp.ClientWebSocketResponse] = []
        
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
    
    async def initialize(self) -> bool:
        """Initialize Discord service with improved token validation"""
        if self._initialization_done:
            return True
            
        self.logger.info("Initializing Discord service", 
                        token_count=len(self.settings.discord_tokens),
                        max_servers=self.settings.max_servers,
                        max_channels_total=self.settings.max_total_channels)
        
        # Create sessions with better configuration
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
        
        # Load server configurations with retry
        await self._discover_servers_with_retry()
        
        self._initialization_done = True
        self.logger.info("Discord service initialized successfully", 
                        valid_tokens=len(self.sessions),
                        servers_found=len(self.servers))
        return True
    
    async def _validate_token_with_retry(self, session: aiohttp.ClientSession, token_index: int) -> bool:
        """Validate token with exponential backoff retry"""
        for attempt in range(self.max_retries):
            try:
                # Progressive delay between attempts
                if attempt > 0:
                    delay = min(self.max_delay, self.base_delay * (2 ** attempt))
                    await asyncio.sleep(delay)
                
                await self.rate_limiter.wait_if_needed(f"token_validate_{token_index}")
                
                # Test basic token validity
                async with session.get('https://discord.com/api/v9/users/@me') as response:
                    if response.status == 429:  # Rate limited
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
                        
                        if response.status in [401, 403]:  # Permanent failures
                            return False
                        
                        continue  # Retry for other status codes
                    
                    user_data = await response.json()
                    self.logger.info("Token valid for user", 
                                   username=user_data.get('username'),
                                   token_index=token_index)
                
                # Test guild access
                async with session.get('https://discord.com/api/v9/users/@me/guilds') as guilds_res:
                    if guilds_res.status == 429:
                        retry_after = float(guilds_res.headers.get('Retry-After', 60))
                        self.logger.warning("Rate limited during guild access test",
                                          token_index=token_index,
                                          retry_after=retry_after)
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(min(retry_after, 60))
                            continue
                        else:
                            return False
                    
                    if guilds_res.status != 200:
                        self.logger.error("Guild access test failed",
                                        token_index=token_index,
                                        status=guilds_res.status)
                        
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
    
    async def _discover_servers_with_retry(self) -> None:
        """Discover servers with improved error handling"""
        if not self.sessions:
            return
        
        for attempt in range(self.max_retries):
            try:
                session = self.sessions[0]  # Use first valid session
                
                await self.rate_limiter.wait_if_needed("discover_guilds")
                
                async with session.get('https://discord.com/api/v9/users/@me/guilds') as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 60))
                        self.logger.error("Rate limited during server discovery", 
                                        retry_after=retry_after,
                                        attempt=attempt + 1)
                        
                        await asyncio.sleep(min(retry_after, 60))
                        continue
                    
                    if response.status != 200:
                        self.logger.error("Failed to fetch guilds", 
                                        status=response.status,
                                        attempt=attempt + 1)
                        
                        if response.status in [401, 403]:  # Permanent failures
                            break
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.base_delay * (2 ** attempt))
                            continue
                        
                        break
                    
                    guilds = await response.json()
                    self.logger.info("Discovered guilds successfully", count=len(guilds))
                    
                    # Process each guild with error isolation
                    for guild in guilds[:self.settings.max_servers]:
                        try:
                            await self._process_guild_with_retry(session, guild)
                        except Exception as e:
                            self.logger.error("Failed to process guild", 
                                            guild_id=guild.get('id'),
                                            guild_name=guild.get('name'),
                                            error=str(e))
                            continue
                    
                    return  # Success, exit retry loop
                    
            except Exception as e:
                self.logger.error("Server discovery error", 
                                error=str(e),
                                attempt=attempt + 1)
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.base_delay * (2 ** attempt))
        
        self.logger.warning("Server discovery completed with some failures")
    
    async def _process_guild_with_retry(self, session: aiohttp.ClientSession, guild_data: dict) -> None:
        """Process guild with retry logic"""
        guild_id = guild_data['id']
        guild_name = guild_data['name']
        
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"guild_{guild_id}")
                
                async with session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels') as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 60))
                        self.logger.warning("Rate limited processing guild", 
                                          guild=guild_name,
                                          retry_after=retry_after,
                                          attempt=attempt + 1)
                        
                        await asyncio.sleep(min(retry_after, 60))
                        continue
                    
                    if response.status != 200:
                        self.logger.warning("Cannot access guild channels", 
                                          guild=guild_name, 
                                          status=response.status,
                                          attempt=attempt + 1)
                        
                        if response.status in [401, 403]:  # Permanent failures
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
                    
                    # Find announcement channels
                    announcement_channels = self._find_announcement_channels(channels)
                    
                    # Add channels to server
                    for channel in announcement_channels[:self.settings.max_channels_per_server]:
                        channel_info = ChannelInfo(
                            channel_id=channel['id'],
                            channel_name=channel['name'],
                            category_id=channel.get('parent_id')
                        )
                        
                        # Test channel accessibility with retry
                        channel_info.http_accessible = await self._test_channel_access_with_retry(
                            session, channel['id']
                        )
                        channel_info.last_checked = datetime.now()
                        
                        server_info.add_channel(channel_info)
                    
                    # Update server stats and status
                    server_info.update_stats()
                    
                    # Store server
                    self.servers[guild_name] = server_info
                    
                    self.logger.info("Processed guild successfully", 
                                   guild=guild_name,
                                   total_channels=len(channels),
                                   announcement_channels=len(announcement_channels),
                                   accessible_channels=server_info.accessible_channel_count)
                    
                    return  # Success
                    
            except Exception as e:
                self.logger.error("Error processing guild", 
                                guild=guild_name, 
                                error=str(e),
                                attempt=attempt + 1)
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.base_delay * (2 ** attempt))
    
    async def _test_channel_access_with_retry(self, session: aiohttp.ClientSession, channel_id: str) -> bool:
        """Test channel access with retry logic"""
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"test_channel_{channel_id}")
                
                async with session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1') as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 60))
                        self.logger.debug("Rate limited testing channel access", 
                                        channel_id=channel_id,
                                        retry_after=retry_after)
                        
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
    
    def _find_announcement_channels(self, channels: List[dict]) -> List[dict]:
        """Find channels that look like announcement channels"""
        announcement_channels = []
        
        for channel in channels:
            if channel.get('type') not in [0, 5]:  # Text channels and announcement channels
                continue
                
            channel_name = channel['name'].lower()
            
            # Look for announcement-related names
            announcement_keywords = [
                'announcement', 'announcements', 'announce', 'news', 
                'updates', 'general', 'important', 'notice'
            ]
            
            if any(keyword in channel_name for keyword in announcement_keywords):
                announcement_channels.append(channel)
        
        return announcement_channels
    
    async def get_recent_messages(self, 
                                 server_name: str, 
                                 channel_id: str, 
                                 limit: int = 10) -> List[DiscordMessage]:
        """Get recent messages with improved error handling"""
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
        if not channel.http_accessible:
            self.logger.warning("Channel not accessible via HTTP", 
                              server=server_name, 
                              channel=channel.channel_name)
            return []
        
        # Use token rotation with failure tracking
        session = self._get_healthy_session()
        if not session:
            self.logger.error("No healthy sessions available")
            return []
        
        messages = []
        
        for attempt in range(self.max_retries):
            try:
                await self.rate_limiter.wait_if_needed(f"messages_{channel_id}")
                
                async with session.get(
                    f'https://discord.com/api/v9/channels/{channel_id}/messages',
                    params={'limit': min(limit, self.settings.max_history_messages)}
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
                        
                        if response.status in [401, 403]:  # Permanent failures
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
                                continue  # Skip empty messages
                                
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
                        channel.last_message_time = messages[0].timestamp
                    
                    self.logger.info("Retrieved messages successfully", 
                                   server=server_name,
                                   channel=channel.channel_name,
                                   message_count=len(messages))
                    
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
    
    def _get_healthy_session(self) -> Optional[aiohttp.ClientSession]:
        """Get a healthy session using round-robin with failure tracking"""
        if not self.sessions:
            return None
        
        # Try to find a session that hasn't failed recently
        attempts = len(self.sessions)
        
        for _ in range(attempts):
            session_index = self.current_token_index
            self.current_token_index = (self.current_token_index + 1) % len(self.sessions)
            
            failure_count = self.token_failure_counts.get(session_index, 0)
            if failure_count < 5:  # Allow sessions with less than 5 recent failures
                return self.sessions[session_index]
        
        # If all sessions have high failure counts, reset and use the first one
        self.token_failure_counts = {i: 0 for i in range(len(self.sessions))}
        return self.sessions[0]
    
    async def start_websocket_monitoring(self) -> None:
        """Start monitoring with HTTP polling (WebSocket disabled due to rate limits)"""
        self.logger.info("Starting HTTP API polling monitoring")
        
        if not self.sessions:
            self.logger.error("No valid sessions for monitoring")
            return
        
        self.running = True
        
        try:
            await self._http_polling_loop()
        except Exception as e:
            self.logger.error("HTTP polling monitoring failed", error=str(e))
        finally:
            self.running = False
    
    async def _http_polling_loop(self) -> None:
        """HTTP polling loop with intelligent scheduling"""
        base_poll_interval = 60  # Poll every 60 seconds
        error_count = 0
        
        while self.running:
            try:
                poll_start = datetime.now()
                
                # Poll each accessible channel
                tasks = []
                for server_name, server_info in self.servers.items():
                    if server_info.status != ServerStatus.ACTIVE:
                        continue
                    
                    for channel_id, channel_info in server_info.accessible_channels.items():
                        task = self._poll_channel_safely(server_name, channel_id)
                        tasks.append(task)
                
                if tasks:
                    # Execute polls with concurrency limit
                    semaphore = asyncio.Semaphore(3)  # Max 3 concurrent polls
                    
                    async def poll_with_semaphore(task):
                        async with semaphore:
                            return await task
                    
                    results = await asyncio.gather(
                        *[poll_with_semaphore(task) for task in tasks],
                        return_exceptions=True
                    )
                    
                    # Count successful polls
                    successful_polls = sum(1 for result in results if result and not isinstance(result, Exception))
                    
                    self.logger.info("Polling cycle completed", 
                                   total_polls=len(tasks),
                                   successful_polls=successful_polls,
                                   duration_seconds=(datetime.now() - poll_start).total_seconds())
                    
                    error_count = 0  # Reset error count on successful cycle
                else:
                    self.logger.debug("No accessible channels to poll")
                
                # Adaptive polling interval
                poll_interval = base_poll_interval
                if error_count > 3:
                    poll_interval = min(300, base_poll_interval * (2 ** min(error_count - 3, 3)))
                
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                error_count += 1
                self.logger.error("Error in polling loop", 
                                error=str(e),
                                error_count=error_count)
                
                # Exponential backoff on repeated errors
                error_delay = min(300, 30 * (2 ** min(error_count, 4)))
                await asyncio.sleep(error_delay)
    
    async def _poll_channel_safely(self, server_name: str, channel_id: str) -> bool:
        """Poll a single channel safely"""
        try:
            messages = await self.get_recent_messages(server_name, channel_id, limit=3)
            
            if messages:
                self.logger.debug("Found new messages during poll", 
                                server=server_name,
                                channel_id=channel_id,
                                message_count=len(messages))
                
                # Here you would typically queue these messages for processing
                # This would be connected to the MessageProcessor
            
            return True
            
        except Exception as e:
            self.logger.error("Error polling channel", 
                            server=server_name,
                            channel_id=channel_id,
                            error=str(e))
            return False
    
    def get_server_stats(self) -> Dict[str, any]:
        """Get statistics for all servers"""
        return {
            "total_servers": len(self.servers),
            "active_servers": len([s for s in self.servers.values() if s.status == ServerStatus.ACTIVE]),
            "total_channels": sum(s.channel_count for s in self.servers.values()),
            "accessible_channels": sum(s.accessible_channel_count for s in self.servers.values()),
            "valid_sessions": len(self.sessions),
            "servers": {name: {
                "status": server.status.value,
                "channels": server.channel_count,
                "accessible_channels": server.accessible_channel_count,
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
        
        self.logger.info("Discord service cleaned up")