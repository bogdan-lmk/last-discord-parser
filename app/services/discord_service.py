# app/services/discord_service.py
import aiohttp
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Optional, Set
import structlog

from ..models.message import DiscordMessage
from ..models.server import ServerInfo, ChannelInfo, ServerStatus
from ..config import Settings
from ..utils.rate_limiter import RateLimiter

class DiscordService:
    """Clean Discord service with dependency injection"""
    
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
        
        # Server tracking
        self.servers: Dict[str, ServerInfo] = {}
        self.websocket_connections: List[aiohttp.ClientWebSocketResponse] = []
        
        # State
        self.running = False
        self._initialization_done = False
    
    async def initialize(self) -> bool:
        """Initialize Discord service with token validation"""
        if self._initialization_done:
            return True
            
        self.logger.info("Initializing Discord service", 
                        token_count=len(self.settings.discord_tokens),
                        max_servers=self.settings.max_servers,
                        max_channels_total=self.settings.max_total_channels)
        
        # Create sessions for each token with retry logic
        max_attempts = 3
        for i, token in enumerate(self.settings.discord_tokens):
            session = aiohttp.ClientSession(
                headers={'Authorization': token},
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            # Validate token with retry
            success = False
            for attempt in range(max_attempts):
                try:
                    if await self._validate_token(session, i):
                        self.sessions.append(session)
                        self.logger.info("Token validated", token_index=i)
                        success = True
                        break
                    else:
                        self.logger.error("Invalid token", token_index=i)
                        break
                except Exception as e:
                    self.logger.error("Token validation error", 
                                    token_index=i, 
                                    error=str(e),
                                    attempt=attempt + 1)
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(1)  # Wait before retry
                    
            if not success:
                await session.close()
                self.logger.error("Token validation failed", token_index=i)
        
        if not self.sessions:
            self.logger.error("No valid Discord tokens available")
            return False
        
        # Load server configurations
        await self._discover_servers()
        
        self._initialization_done = True
        self.logger.info("Discord service initialized", 
                        valid_tokens=len(self.sessions),
                        servers_found=len(self.servers))
        return True
    
    async def _validate_token(self, session: aiohttp.ClientSession, token_index: int) -> bool:
        """Validate Discord token and permissions"""
        try:
            await self.rate_limiter.wait_if_needed(f"token_{token_index}")
            
            # First check basic token validity
            async with session.get('https://discord.com/api/v9/users/@me') as response:
                if response.status != 200:
                    self.logger.error("Invalid token", token_index=token_index)
                    self.rate_limiter.record_error()
                    return False
                
                user_data = await response.json()
                self.logger.info("Token valid for user", 
                               username=user_data.get('username'),
                               token_index=token_index)
            
            # Then check required permissions
            if not await self._validate_token_permissions(session):
                self.logger.error("Token missing required permissions", token_index=token_index)
                self.rate_limiter.record_error()
                return False
            
            # Finally check guild access
            async with session.get('https://discord.com/api/v9/users/@me/guilds') as guilds_res:
                if guilds_res.status != 200:
                    self.logger.error("Cannot access guilds", token_index=token_index)
                    self.rate_limiter.record_error()
                    return False
                
                guilds = await guilds_res.json()
                if not guilds:
                    self.logger.error("Token has no guild access", token_index=token_index)
                    self.rate_limiter.record_error()
                    return False
            
            self.rate_limiter.record_success()
            return True
                
        except Exception as e:
            self.logger.error("Token validation failed", 
                            token_index=token_index, 
                            error=str(e))
            self.rate_limiter.record_error()
            return False

    async def _validate_token_permissions(self, session: aiohttp.ClientSession) -> bool:
        """Validate token has required permissions for HTTP API access"""
        try:
            # Проверяем доступ к гильдиям
            async with session.get('https://discord.com/api/v9/users/@me/guilds') as guilds_res:
                if guilds_res.status != 200:
                    self.logger.warning("Token cannot access guilds", status=guilds_res.status)
                    return False
                
                guilds = await guilds_res.json()
                if not guilds:
                    self.logger.warning("Token has no guild access")
                    return False
                
                self.logger.info("Token has access to guilds", guild_count=len(guilds))
                
                # Проверяем информацию о пользователе
                async with session.get('https://discord.com/api/v9/users/@me') as user_res:
                    if user_res.status != 200:
                        self.logger.warning("Cannot get user info", status=user_res.status)
                        return False
                    
                    user_data = await user_res.json()
                    is_bot = user_data.get('bot', False)
                    
                    if is_bot:
                        self.logger.info("Detected bot token")
                        # Для бот токенов можно проверить интенты, но это не обязательно для HTTP API
                    else:
                        self.logger.info("Detected user token - using HTTP API only")
                
                # Тестируем доступ к каналам первой гильдии
                if guilds:
                    test_guild = guilds[0]
                    guild_id = test_guild['id']
                    guild_name = test_guild.get('name', 'Unknown')
                    
                    self.logger.info("Testing access to guild", 
                                   guild_name=guild_name, 
                                   guild_id=guild_id)
                    
                    async with session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels') as channels_res:
                        if channels_res.status == 200:
                            channels = await channels_res.json()
                            self.logger.info("Successfully accessed guild channels", 
                                           guild=guild_name,
                                           channel_count=len(channels))
                            return True
                        else:
                            self.logger.warning("Cannot access guild channels", 
                                              guild=guild_name,
                                              status=channels_res.status)
                            
                            # Если не можем получить каналы первой гильдии, попробуем другие
                            for guild in guilds[1:3]:  # Проверим еще 2 гильдии
                                guild_id = guild['id']
                                guild_name = guild.get('name', 'Unknown')
                                
                                async with session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels') as test_res:
                                    if test_res.status == 200:
                                        channels = await test_res.json()
                                        self.logger.info("Found accessible guild", 
                                                       guild=guild_name,
                                                       channel_count=len(channels))
                                        return True
                            
                            self.logger.error("No accessible guilds found")
                            return False
                
                return True
                    
        except Exception as e:
            self.logger.error("Permission validation failed", error=str(e))
            return False
    
    async def _discover_servers(self) -> None:
        """Discover available Discord servers and their announcement channels"""
        if not self.sessions:
            return
        
        session = self.sessions[0]  # Use first valid session
        
        try:
            await self.rate_limiter.wait_if_needed("discover_guilds")
            
            async with session.get('https://discord.com/api/v9/users/@me/guilds') as response:
                if response.status != 200:
                    self.logger.error("Failed to fetch guilds", status=response.status)
                    return
                
                guilds = await response.json()
                self.logger.info("Discovered guilds", count=len(guilds))
                
                # Process each guild
                for guild in guilds[:self.settings.max_servers]:  # Respect server limit
                    await self._process_guild(session, guild)
                    
        except Exception as e:
            self.logger.error("Server discovery failed", error=str(e))
    
    async def _process_guild(self, session: aiohttp.ClientSession, guild_data: dict) -> None:
        """Process individual guild and find announcement channels"""
        guild_id = guild_data['id']
        guild_name = guild_data['name']
        
        try:
            await self.rate_limiter.wait_if_needed(f"guild_{guild_id}")
            
            # Get guild channels
            async with session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels') as response:
                if response.status != 200:
                    self.logger.warning("Cannot access guild channels", 
                                      guild=guild_name, 
                                      status=response.status)
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
                    
                    # Test channel accessibility
                    channel_info.http_accessible = await self._test_channel_access(
                        session, channel['id']
                    )
                    channel_info.last_checked = datetime.now()
                    
                    server_info.add_channel(channel_info)
                
                # Update server stats and status
                server_info.update_stats()
                
                # Store server
                self.servers[guild_name] = server_info
                
                self.logger.info("Processed guild", 
                               guild=guild_name,
                               total_channels=len(channels),
                               announcement_channels=len(announcement_channels),
                               accessible_channels=server_info.accessible_channel_count)
                
        except Exception as e:
            self.logger.error("Failed to process guild", 
                            guild=guild_name, 
                            error=str(e))
    
    def _find_announcement_channels(self, channels: List[dict]) -> List[dict]:
        """Find channels that look like announcement channels"""
        announcement_channels = []
        
        for channel in channels:
            if channel.get('type') not in [0, 5]:  # Text channels and announcement channels
                continue
                
            channel_name = channel['name'].lower()
            
            # Look for announcement-related names
            if (channel_name.endswith('announcement') or 
                channel_name.endswith('announcements') or
                'announce' in channel_name or
                'news' in channel_name):
                announcement_channels.append(channel)
        
        return announcement_channels
    
    async def _test_channel_access(self, session: aiohttp.ClientSession, channel_id: str) -> bool:
        """Test if we can access a channel"""
        try:
            await self.rate_limiter.wait_if_needed(f"test_channel_{channel_id}")
            
            async with session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1') as response:
                result = response.status == 200
                
                if result:
                    self.rate_limiter.record_success()
                else:
                    self.rate_limiter.record_error()
                    
                return result
                
        except Exception:
            self.rate_limiter.record_error()
            return False
    
    async def get_recent_messages(self, 
                                 server_name: str, 
                                 channel_id: str, 
                                 limit: int = 10) -> List[DiscordMessage]:
        """Get recent messages from a channel"""
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
        
        # Use token rotation for requests
        session = self._get_next_session()
        messages = []
        
        try:
            await self.rate_limiter.wait_if_needed(f"messages_{channel_id}")
            
            async with session.get(
                f'https://discord.com/api/v9/channels/{channel_id}/messages',
                params={'limit': min(limit, self.settings.max_history_messages)}
            ) as response:
                
                if response.status != 200:
                    self.logger.error("Failed to fetch messages", 
                                    channel_id=channel_id,
                                    status=response.status)
                    self.rate_limiter.record_error()
                    return []
                
                raw_messages = await response.json()
                self.rate_limiter.record_success()
                
                # Convert to DiscordMessage objects
                for raw_msg in raw_messages:
                    try:
                        message = DiscordMessage(
                            content=raw_msg['content'],
                            timestamp=datetime.fromisoformat(raw_msg['timestamp'].replace('Z', '+00:00')),
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
                
                self.logger.info("Retrieved messages", 
                               server=server_name,
                               channel=channel.channel_name,
                               message_count=len(messages))
                
                return sorted(messages, key=lambda x: x.timestamp)
                
        except Exception as e:
            self.logger.error("Error retrieving messages", 
                            server=server_name,
                            channel_id=channel_id,
                            error=str(e))
            self.rate_limiter.record_error()
            return []
    
    def _get_next_session(self) -> aiohttp.ClientSession:
        """Get next session using round-robin"""
        session = self.sessions[self.current_token_index]
        self.current_token_index = (self.current_token_index + 1) % len(self.sessions)
        return session
    
    async def start_websocket_monitoring(self) -> None:
        """WebSocket monitoring disabled - using HTTP API only"""
        self.logger.info("WebSocket monitoring disabled - using HTTP API polling instead")
        
        if not self.sessions:
            self.logger.error("No valid sessions for monitoring")
            return
        
        self.running = True
        self.logger.info("Starting HTTP API polling monitoring")
        
        # Start periodic polling instead of WebSocket
        try:
            await self._http_polling_loop()
        except Exception as e:
            self.logger.error("HTTP polling monitoring failed", error=str(e))
        finally:
            self.running = False
    
    async def _http_polling_loop(self) -> None:
        """HTTP polling loop instead of WebSocket"""
        poll_interval = 30  # Poll every 30 seconds
        
        while self.running:
            try:
                # Poll each accessible channel for new messages
                for server_name, server_info in self.servers.items():
                    if server_info.status != ServerStatus.ACTIVE:
                        continue
                    
                    for channel_id, channel_info in server_info.accessible_channels.items():
                        try:
                            # Get recent messages (limit to 5 to avoid rate limits)
                            messages = await self.get_recent_messages(
                                server_name, 
                                channel_id, 
                                limit=5
                            )
                            
                            if messages:
                                self.logger.debug("Polled messages", 
                                                server=server_name,
                                                channel=channel_info.channel_name,
                                                message_count=len(messages))
                            
                        except Exception as e:
                            self.logger.error("Error polling channel", 
                                            server=server_name,
                                            channel_id=channel_id,
                                            error=str(e))
                
                # Wait before next poll
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                self.logger.error("Error in polling loop", error=str(e))
                await asyncio.sleep(60)  # Wait longer on error
    
    async def _websocket_connection_loop(self, session: aiohttp.ClientSession, token_index: int) -> None:
        """WebSocket connection loop for a single token"""
        while self.running:
            ws = None
            try:
                # Get gateway URL
                async with session.get('https://discord.com/api/v9/gateway') as response:
                    gateway_data = await response.json()
                    gateway_url = gateway_data['url']
                
                # Connect to WebSocket with timeout
                timeout = aiohttp.ClientTimeout(total=300, sock_read=60)  # 5 min total, 1 min read
                ws = await session.ws_connect(
                    f"{gateway_url}/?v=9&encoding=json",
                    timeout=timeout,
                    heartbeat=30
                )
                self.websocket_connections.append(ws)
                self.logger.info("WebSocket connected", token_index=token_index)
                
                try:
                    await asyncio.wait_for(
                        self._handle_websocket_messages(ws, token_index),
                        timeout=3600  # 1 hour max
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("WebSocket timeout, reconnecting...")
                    continue
                
            except Exception as e:
                self.logger.error("WebSocket connection error", 
                                token_index=token_index,
                                error=str(e))
                
                if self.running:
                    await asyncio.sleep(self.settings.websocket_reconnect_delay)
            finally:
                if ws and not ws.closed:
                    await ws.close()
                if ws in self.websocket_connections:
                    self.websocket_connections.remove(ws)
    
    async def _handle_websocket_messages(self, ws: aiohttp.ClientWebSocketResponse, token_index: int) -> None:
        """Handle WebSocket messages"""
        heartbeat_task = None
        
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    
                    if data['op'] == 10:  # HELLO
                        heartbeat_interval = data['d']['heartbeat_interval']
                        heartbeat_task = asyncio.create_task(
                            self._send_heartbeat(ws, heartbeat_interval)
                        )
                        await self._identify(ws, token_index)
                        
                    elif data['op'] == 0 and data['t'] == 'MESSAGE_CREATE':
                        await self._handle_new_message(data['d'])
                        
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error("WebSocket error", 
                                    token_index=token_index,
                                    error=ws.exception())
                    break
                    
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
    
    async def _send_heartbeat(self, ws: aiohttp.ClientWebSocketResponse, interval: int) -> None:
        """Send periodic heartbeat"""
        try:
            while not ws.closed:
                await ws.send_str(json.dumps({"op": 1, "d": None}))
                await asyncio.sleep(interval / 1000)
        except asyncio.CancelledError:
            pass
    
    async def _identify(self, ws: aiohttp.ClientWebSocketResponse, token_index: int) -> None:
        """Send IDENTIFY payload"""
        identify_payload = {
            "op": 2,
            "d": {
                "token": self.settings.discord_tokens[token_index],
                "properties": {
                    "$os": "linux",
                    "$browser": "discord_parser_mvp",
                    "$device": "discord_parser_mvp"
                },
                "compress": False,
                "large_threshold": 50,
                "intents": 33281  # GUILDS + GUILD_MESSAGES + MESSAGE_CONTENT
            }
        }
        await ws.send_str(json.dumps(identify_payload))
    
    async def _handle_new_message(self, message_data: dict) -> None:
        """Handle new message from WebSocket"""
        # This will be connected to MessageProcessor via callback
        # For now, just log
        channel_id = message_data['channel_id']
        content = message_data.get('content', '')
        
        if content.strip():  # Only process non-empty messages
            self.logger.info("New message received via WebSocket", 
                           channel_id=channel_id,
                           content_preview=content[:50])
    
    def get_server_stats(self) -> Dict[str, any]:
        """Get statistics for all servers"""
        return {
            "total_servers": len(self.servers),
            "active_servers": len([s for s in self.servers.values() if s.status == ServerStatus.ACTIVE]),
            "total_channels": sum(s.channel_count for s in self.servers.values()),
            "accessible_channels": sum(s.accessible_channel_count for s in self.servers.values()),
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
            await session.close()
        
        self.logger.info("Discord service cleaned up")