# app/services/discord_service.py
import aiohttp
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Optional, Set, Callable
import structlog

from ..models.message import DiscordMessage
from ..models.server import ServerInfo, ChannelInfo, ServerStatus
from ..config import Settings
from ..utils.rate_limiter import RateLimiter

class DiscordService:
    """Discord service with real-time WebSocket monitoring"""
    
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
        
        # Real-time message callbacks
        self.message_callbacks: List[Callable[[DiscordMessage], None]] = []
        
        # WebSocket state management
        self.websocket_states: Dict[int, dict] = {}  # token_index -> state
        self.sequence_numbers: Dict[int, Optional[int]] = {}  # token_index -> sequence
        self.session_ids: Dict[int, Optional[str]] = {}  # token_index -> session_id
        
        # Monitoring channels (channel_id -> server_name mapping)
        self.monitored_channels: Dict[str, str] = {}
        
        # State
        self.running = False
        self._initialization_done = False
        
        # Message deduplication
        self.processed_message_ids: Set[str] = set()
        self.message_id_cleanup_interval = 3600  # Clean old IDs every hour
        self.last_cleanup = datetime.now()
    
    def add_message_callback(self, callback: Callable[[DiscordMessage], None]) -> None:
        """Add callback for real-time messages"""
        self.message_callbacks.append(callback)
        self.logger.info("Added message callback", total_callbacks=len(self.message_callbacks))
    
    async def initialize(self) -> bool:
        """Initialize Discord service with token validation"""
        if self._initialization_done:
            return True
            
        self.logger.info("Initializing Discord service with real-time monitoring", 
                        token_count=len(self.settings.discord_tokens),
                        max_servers=self.settings.max_servers,
                        max_channels_total=self.settings.max_total_channels)
        
        # Create sessions for each token with retry logic
        max_attempts = 3
        for i, token in enumerate(self.settings.discord_tokens):
            session = aiohttp.ClientSession(
                headers={'Authorization': f'Bot {token}' if not token.startswith('Bot ') else token},
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            # Initialize WebSocket state
            self.websocket_states[i] = {
                'heartbeat_interval': None,
                'heartbeat_task': None,
                'connected': False,
                'ready': False
            }
            self.sequence_numbers[i] = None
            self.session_ids[i] = None
            
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
        
        # Load server configurations and build monitoring map
        await self._discover_servers()
        await self._build_monitoring_map()
        
        self._initialization_done = True
        self.logger.info("Discord service initialized", 
                        valid_tokens=len(self.sessions),
                        servers_found=len(self.servers),
                        monitored_channels=len(self.monitored_channels))
        return True
    
    async def _validate_token(self, session: aiohttp.ClientSession, token_index: int) -> bool:
        """Validate Discord token and permissions"""
        try:
            await self.rate_limiter.wait_if_needed(f"token_{token_index}")
            
            # First check basic token validity
            async with session.get('https://discord.com/api/v10/users/@me') as response:
                if response.status != 200:
                    self.logger.error("Invalid token", token_index=token_index, status=response.status)
                    self.rate_limiter.record_error()
                    return False
                
                user_data = await response.json()
                self.logger.info("Token valid for user", 
                               username=user_data.get('username'),
                               user_id=user_data.get('id'),
                               token_index=token_index)
            
            # Check guild access
            async with session.get('https://discord.com/api/v10/users/@me/guilds') as guilds_res:
                if guilds_res.status != 200:
                    self.logger.error("Cannot access guilds", token_index=token_index)
                    self.rate_limiter.record_error()
                    return False
                
                guilds = await guilds_res.json()
                if not guilds:
                    self.logger.error("Token has no guild access", token_index=token_index)
                    self.rate_limiter.record_error()
                    return False
                
                self.logger.info("Token has guild access", 
                               token_index=token_index,
                               guild_count=len(guilds))
            
            self.rate_limiter.record_success()
            return True
                
        except Exception as e:
            self.logger.error("Token validation failed", 
                            token_index=token_index, 
                            error=str(e))
            self.rate_limiter.record_error()
            return False
    
    async def _discover_servers(self) -> None:
        """Discover available Discord servers and their announcement channels"""
        if not self.sessions:
            return
        
        session = self.sessions[0]  # Use first valid session
        
        try:
            await self.rate_limiter.wait_if_needed("discover_guilds")
            
            async with session.get('https://discord.com/api/v10/users/@me/guilds') as response:
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
            async with session.get(f'https://discord.com/api/v10/guilds/{guild_id}/channels') as response:
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
                    channel_info.websocket_accessible = True  # Assume WebSocket can access
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
                'news' in channel_name or
                'general' in channel_name):  # Add general channels too
                announcement_channels.append(channel)
        
        return announcement_channels
    
    async def _build_monitoring_map(self) -> None:
        """Build mapping of channel IDs to server names for WebSocket monitoring"""
        self.monitored_channels.clear()
        
        for server_name, server_info in self.servers.items():
            for channel_id, channel_info in server_info.channels.items():
                if channel_info.is_accessible:
                    self.monitored_channels[channel_id] = server_name
        
        self.logger.info("Built monitoring map", 
                        monitored_channels=len(self.monitored_channels),
                        servers=len(self.servers))
    
    async def _test_channel_access(self, session: aiohttp.ClientSession, channel_id: str) -> bool:
        """Test if we can access a channel"""
        try:
            await self.rate_limiter.wait_if_needed(f"test_channel_{channel_id}")
            
            async with session.get(f'https://discord.com/api/v10/channels/{channel_id}/messages?limit=1') as response:
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
                f'https://discord.com/api/v10/channels/{channel_id}/messages',
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
        """Start real-time WebSocket monitoring"""
        if not self.sessions:
            self.logger.error("No valid sessions for WebSocket monitoring")
            return
        
        self.running = True
        self.logger.info("Starting real-time WebSocket monitoring",
                        sessions=len(self.sessions),
                        monitored_channels=len(self.monitored_channels))
        
        # Start WebSocket connections for each token
        websocket_tasks = []
        for i, session in enumerate(self.sessions):
            task = asyncio.create_task(self._websocket_connection_loop(session, i))
            websocket_tasks.append(task)
        
        try:
            # Wait for all WebSocket connections
            await asyncio.gather(*websocket_tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error("WebSocket monitoring failed", error=str(e))
        finally:
            self.running = False
    
    async def _websocket_connection_loop(self, session: aiohttp.ClientSession, token_index: int) -> None:
        """WebSocket connection loop for a single token"""
        while self.running:
            ws = None
            try:
                # Get gateway URL
                async with session.get('https://discord.com/api/v10/gateway/bot') as response:
                    if response.status != 200:
                        self.logger.error("Failed to get gateway", 
                                        token_index=token_index, 
                                        status=response.status)
                        await asyncio.sleep(30)
                        continue
                    
                    gateway_data = await response.json()
                    gateway_url = gateway_data['url']
                
                # Connect to WebSocket
                self.logger.info("Connecting to Discord Gateway", 
                               token_index=token_index,
                               gateway_url=gateway_url)
                
                ws = await session.ws_connect(
                    f"{gateway_url}/?v=10&encoding=json",
                    timeout=aiohttp.ClientTimeout(total=300),
                    heartbeat=30
                )
                
                self.websocket_connections.append(ws)
                self.websocket_states[token_index]['connected'] = True
                
                self.logger.info("WebSocket connected", token_index=token_index)
                
                # Handle WebSocket messages
                await self._handle_websocket_messages(ws, token_index)
                
            except Exception as e:
                self.logger.error("WebSocket connection error", 
                                token_index=token_index,
                                error=str(e))
                
                # Reset state
                self.websocket_states[token_index]['connected'] = False
                self.websocket_states[token_index]['ready'] = False
                
                if self.running:
                    await asyncio.sleep(self.settings.websocket_reconnect_delay)
            finally:
                if ws and not ws.closed:
                    await ws.close()
                if ws in self.websocket_connections:
                    self.websocket_connections.remove(ws)
                
                # Stop heartbeat
                heartbeat_task = self.websocket_states[token_index].get('heartbeat_task')
                if heartbeat_task:
                    heartbeat_task.cancel()
    
    async def _handle_websocket_messages(self, ws: aiohttp.ClientWebSocketResponse, token_index: int) -> None:
        """Handle WebSocket messages"""
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._process_websocket_event(ws, data, token_index)
                    except json.JSONDecodeError as e:
                        self.logger.error("Failed to decode WebSocket message", 
                                        token_index=token_index,
                                        error=str(e))
                        
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error("WebSocket error", 
                                    token_index=token_index,
                                    error=ws.exception())
                    break
                    
        except Exception as e:
            self.logger.error("Error handling WebSocket messages", 
                            token_index=token_index,
                            error=str(e))
    
    async def _process_websocket_event(self, ws: aiohttp.ClientWebSocketResponse, data: dict, token_index: int) -> None:
        """Process individual WebSocket event"""
        op = data.get('op')
        event_type = data.get('t')
        event_data = data.get('d', {})
        sequence = data.get('s')
        
        # Update sequence number
        if sequence is not None:
            self.sequence_numbers[token_index] = sequence
        
        if op == 10:  # HELLO
            heartbeat_interval = event_data['heartbeat_interval']
            self.websocket_states[token_index]['heartbeat_interval'] = heartbeat_interval
            
            # Start heartbeat
            heartbeat_task = asyncio.create_task(
                self._send_heartbeat(ws, heartbeat_interval, token_index)
            )
            self.websocket_states[token_index]['heartbeat_task'] = heartbeat_task
            
            # Send IDENTIFY
            await self._identify(ws, token_index)
            
        elif op == 0:  # DISPATCH
            if event_type == 'READY':
                self.session_ids[token_index] = event_data.get('session_id')
                self.websocket_states[token_index]['ready'] = True
                self.logger.info("WebSocket ready", 
                               token_index=token_index,
                               session_id=self.session_ids[token_index])
                
            elif event_type == 'MESSAGE_CREATE':
                await self._handle_new_message(event_data, token_index)
                
            elif event_type == 'GUILD_CREATE':
                self.logger.info("Guild available", 
                               token_index=token_index,
                               guild_name=event_data.get('name'))
                
        elif op == 1:  # HEARTBEAT
            await self._send_heartbeat_ack(ws, token_index)
            
        elif op == 7:  # RECONNECT
            self.logger.info("Received reconnect request", token_index=token_index)
            # Will be handled by connection loop
            
        elif op == 9:  # INVALID_SESSION
            self.logger.warning("Invalid session", token_index=token_index)
            # Reset session and reconnect
            self.session_ids[token_index] = None
            await asyncio.sleep(5)  # Wait before reconnecting
    
    async def _send_heartbeat(self, ws: aiohttp.ClientWebSocketResponse, interval: int, token_index: int) -> None:
        """Send periodic heartbeat"""
        try:
            while not ws.closed and self.running:
                sequence = self.sequence_numbers.get(token_index)
                heartbeat_payload = {
                    "op": 1,
                    "d": sequence
                }
                
                await ws.send_str(json.dumps(heartbeat_payload))
                self.logger.debug("Sent heartbeat", 
                                token_index=token_index,
                                sequence=sequence)
                
                await asyncio.sleep(interval / 1000)
                
        except asyncio.CancelledError:
            self.logger.debug("Heartbeat cancelled", token_index=token_index)
        except Exception as e:
            self.logger.error("Heartbeat error", 
                            token_index=token_index,
                            error=str(e))
    
    async def _send_heartbeat_ack(self, ws: aiohttp.ClientWebSocketResponse, token_index: int) -> None:
        """Send heartbeat ACK"""
        sequence = self.sequence_numbers.get(token_index)
        heartbeat_payload = {
            "op": 1,
            "d": sequence
        }
        await ws.send_str(json.dumps(heartbeat_payload))
    
    async def _identify(self, ws: aiohttp.ClientWebSocketResponse, token_index: int) -> None:
        """Send IDENTIFY payload"""
        token = self.settings.discord_tokens[token_index]
        
        identify_payload = {
            "op": 2,
            "d": {
                "token": token if token.startswith('Bot ') else f'Bot {token}',
                "properties": {
                    "$os": "linux",
                    "$browser": "discord_parser_realtime",
                    "$device": "discord_parser_realtime"
                },
                "compress": False,
                "large_threshold": 50,
                "intents": 513  # GUILDS (1) + GUILD_MESSAGES (512) = 513
            }
        }
        
        await ws.send_str(json.dumps(identify_payload))
        self.logger.info("Sent IDENTIFY", token_index=token_index)
    
    async def _handle_new_message(self, message_data: dict, token_index: int) -> None:
        """Handle new message from WebSocket"""
        try:
            channel_id = message_data.get('channel_id')
            message_id = message_data.get('id')
            content = message_data.get('content', '')
            author_data = message_data.get('author', {})
            
            # Skip if empty content or bot message
            if not content.strip() or author_data.get('bot', False):
                return
            
            # Check if we're monitoring this channel
            if channel_id not in self.monitored_channels:
                self.logger.debug("Message from unmonitored channel", 
                                channel_id=channel_id,
                                token_index=token_index)
                return
            
            # Check for duplicates
            if message_id in self.processed_message_ids:
                self.logger.debug("Duplicate message ignored", message_id=message_id)
                return
            
            server_name = self.monitored_channels[channel_id]
            server_info = self.servers.get(server_name)
            
            if not server_info or channel_id not in server_info.channels:
                self.logger.warning("Server or channel not found", 
                                  server=server_name,
                                  channel_id=channel_id)
                return
            
            channel_info = server_info.channels[channel_id]
            
            # Create Discord message object
            timestamp_str = message_data.get('timestamp', datetime.now().isoformat())
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            discord_message = DiscordMessage(
                content=content,
                timestamp=timestamp,
                server_name=server_name,
                channel_name=channel_info.channel_name,
                author=author_data.get('username', 'Unknown'),
                message_id=message_id,
                channel_id=channel_id,
                guild_id=server_info.guild_id
            )
            
            # Mark as processed
            self.processed_message_ids.add(message_id)
            
            # Update statistics
            channel_info.message_count += 1
            channel_info.last_message_time = timestamp
            server_info.total_messages += 1
            server_info.last_activity = timestamp
            
            self.logger.info("Real-time message received", 
                           server=server_name,
                           channel=channel_info.channel_name,
                           author=discord_message.author,
                           content_preview=content[:50],
                           token_index=token_index)
            
            # Call all registered callbacks
            for callback in self.message_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(discord_message)
                    else:
                        callback(discord_message)
                except Exception as e:
                    self.logger.error("Error in message callback", 
                                    server=server_name,
                                    callback=callback.__name__,
                                    error=str(e))
            
            # Cleanup old message IDs periodically
            await self._cleanup_processed_message_ids()
            
        except Exception as e:
            self.logger.error("Error handling new message", 
                            token_index=token_index,
                            error=str(e))
    
    async def _cleanup_processed_message_ids(self) -> None:
        """Cleanup old processed message IDs to prevent memory leaks"""
        now = datetime.now()
        if (now - self.last_cleanup).total_seconds() > self.message_id_cleanup_interval:
            # Keep only recent IDs (for deduplication)
            # Discord message IDs are snowflakes, we can use them to determine age
            # For simplicity, we'll just limit the set size
            if len(self.processed_message_ids) > 10000:
                # Keep only the most recent 5000 IDs
                sorted_ids = sorted(self.processed_message_ids)
                self.processed_message_ids = set(sorted_ids[-5000:])
                
                self.logger.info("Cleaned up processed message IDs", 
                               remaining_count=len(self.processed_message_ids))
            
            self.last_cleanup = now
    
    def get_server_stats(self) -> Dict[str, any]:
        """Get statistics for all servers"""
        return {
            "total_servers": len(self.servers),
            "active_servers": len([s for s in self.servers.values() if s.status == ServerStatus.ACTIVE]),
            "total_channels": sum(s.channel_count for s in self.servers.values()),
            "accessible_channels": sum(s.accessible_channel_count for s in self.servers.values()),
            "monitored_channels": len(self.monitored_channels),
            "websocket_connections": len(self.websocket_connections),
            "processed_messages": len(self.processed_message_ids),
            "servers": {name: {
                "status": server.status.value,
                "channels": server.channel_count,
                "accessible_channels": server.accessible_channel_count,
                "total_messages": server.total_messages,
                "last_sync": server.last_sync.isoformat() if server.last_sync else None,
                "last_activity": server.last_activity.isoformat() if server.last_activity else None
            } for name, server in self.servers.items()}
        }
    
    async def cleanup(self) -> None:
        """Clean up resources"""
        self.running = False
        
        # Cancel all heartbeat tasks
        for token_index, state in self.websocket_states.items():
            heartbeat_task = state.get('heartbeat_task')
            if heartbeat_task:
                heartbeat_task.cancel()
        
        # Close all WebSocket connections
        for ws in self.websocket_connections:
            if not ws.closed:
                await ws.close()
        
        # Close all HTTP sessions
        for session in self.sessions:
            await session.close()
        
        self.logger.info("Discord service cleaned up",
                        processed_messages=len(self.processed_message_ids))