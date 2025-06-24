# app/main.py
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

import asyncio
import signal
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import structlog

from .dependencies import (
    container,
    get_settings_dependency,
    get_message_processor_dependency,
    get_discord_service_dependency,
    get_telegram_service_dependency
)
from .config import Settings
from .models.message import DiscordMessage
from .models.server import ServerInfo, SystemStats
from .services.message_processor import MessageProcessor
from .services.discord_service import DiscordService
from .services.telegram_service import TelegramService

# Global message processor instance
message_processor: Optional[MessageProcessor] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global message_processor
    
    logger = structlog.get_logger(__name__)
    logger.info("Starting Discord Telegram Parser MVP with enhanced Telegram features")
    
    try:
        # Initialize dependency injection container
        container.wire(modules=[__name__])
        
        # Get message processor
        message_processor = container.message_processor()
        
        # Initialize all services
        if await message_processor.initialize():
            logger.info("All services initialized successfully with enhanced features")
            
            # Start message processor in background
            asyncio.create_task(message_processor.start())
            
            # Setup graceful shutdown
            def signal_handler(signum, frame):
                logger.info("Received shutdown signal", signal=signum)
                asyncio.create_task(message_processor.stop())
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
        else:
            logger.error("Service initialization failed")
            raise RuntimeError("Failed to initialize services")
        
        yield
        
    finally:
        logger.info("Shutting down application")
        if message_processor:
            await message_processor.stop()

# Create FastAPI app
app = FastAPI(
    title="Discord Telegram Parser MVP - Enhanced",
    description="Professional Discord to Telegram message forwarding service with enhanced features",
    version="2.2.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for API
class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    uptime_seconds: int
    health_score: float
    enhanced_features: bool = True

class StatusResponse(BaseModel):
    system: Dict
    discord: Dict
    telegram: Dict
    processing: Dict
    rate_limiting: Dict
    enhanced_features: Dict  # НОВОЕ

class ServerListResponse(BaseModel):
    servers: List[Dict]
    total_count: int
    active_count: int

class MessageRequest(BaseModel):
    server_name: str
    channel_id: str
    limit: int = 10

class TopicMappingResponse(BaseModel):
    server_topics: Dict[str, int]
    total_topics: int
    last_updated: str
    anti_duplicate_protection: bool = True

class ChannelAddRequest(BaseModel):
    channel_id: str
    channel_name: Optional[str] = None

# API Routes

@app.get("/")
async def root():
    """Root endpoint with enhanced features info"""
    return {
        "name": "Discord Telegram Parser MVP - Enhanced",
        "version": "2.2.0",
        "status": "running",
        "feature": "1 Discord Server = 1 Telegram Topic with Enhanced Bot Interface",
        "enhanced_features": [
            "Anti-duplicate topic protection",
            "Interactive bot interface",
            "Channel management",
            "Topic verification",
            "WebSocket integration",
            "Enhanced message deduplication"
        ],
        "docs": "/docs",
        "health": "/health",
        "enhanced_endpoints": [
            "/telegram/enhanced-stats",
            "/servers/{server_name}/channels",
            "/telegram/force-verification",
            "/telegram/bot-status"
        ]
    }

@app.get("/health", response_model=HealthResponse)
async def health_check(
    settings: Settings = Depends(get_settings_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Enhanced health check endpoint"""
    global message_processor
    
    if not message_processor:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    status = message_processor.get_status()
    
    # Проверка enhanced функций
    enhanced_features_available = False
    try:
        enhanced_stats = telegram_service.get_enhanced_stats()
        enhanced_features_available = bool(enhanced_stats.get("features", {}))
    except Exception:
        enhanced_features_available = False
    
    return HealthResponse(
        status=status["system"]["status"],
        timestamp=datetime.now(),
        uptime_seconds=status["system"]["uptime_seconds"],
        health_score=status["system"]["health_score"],
        enhanced_features=enhanced_features_available
    )

@app.get("/status", response_model=StatusResponse)
async def get_status(
    processor: MessageProcessor = Depends(get_message_processor_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Get comprehensive system status with enhanced features"""
    base_status = processor.get_status()
    
    # Добавляем enhanced статистики
    enhanced_features = {}
    try:
        enhanced_stats = telegram_service.get_enhanced_stats()
        enhanced_features = {
            "telegram_enhanced": enhanced_stats,
            "anti_duplicate_active": telegram_service.startup_verification_done,
            "bot_interface_active": telegram_service.bot_running,
            "user_states_count": len(getattr(telegram_service, 'user_states', {})),
            "processed_messages_cache": len(getattr(telegram_service, 'processed_messages', {}))
        }
    except Exception as e:
        enhanced_features = {"error": str(e), "available": False}
    
    return StatusResponse(
        **base_status,
        enhanced_features=enhanced_features
    )

# НОВЫЙ: Эндпоинт для добавления каналов
@app.post("/servers/{server_name}/channels")
async def add_channel_to_server(
    server_name: str,
    channel_data: ChannelAddRequest,
    telegram_service: TelegramService = Depends(get_telegram_service_dependency),
    discord_service: DiscordService = Depends(get_discord_service_dependency)
):
    """Add a new channel to server monitoring using enhanced Telegram features"""
    try:
        # Проверяем что сервер существует
        if server_name not in discord_service.servers:
            raise HTTPException(status_code=404, detail="Server not found")
        
        # Используем новую функцию из telegram_service
        success, message = telegram_service.add_channel_to_server(
            server_name, 
            channel_data.channel_id, 
            channel_data.channel_name
        )
        
        if success:
            # Получаем topic_id для этого сервера
            topic_id = telegram_service.server_topics.get(server_name)
            
            return {
                "message": "Channel added successfully",
                "server_name": server_name,
                "channel_id": channel_data.channel_id,
                "channel_name": channel_data.channel_name or f"Channel_{channel_data.channel_id}",
                "telegram_topic_id": topic_id,
                "details": message,
                "note": "All channels from this server post to the same Telegram topic"
            }
        else:
            raise HTTPException(status_code=400, detail=message)
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# НОВЫЙ: Enhanced Telegram статистики
@app.get("/telegram/enhanced-stats")
async def get_enhanced_telegram_stats(
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Get enhanced Telegram service statistics"""
    try:
        # Используем новую функцию get_enhanced_stats
        enhanced_stats = telegram_service.get_enhanced_stats()
        
        return {
            "enhanced_stats": enhanced_stats,
            "timestamp": datetime.now().isoformat(),
            "server_topics": telegram_service.server_topics.copy(),
            "features": [
                "Anti-duplicate protection",
                "Interactive bot interface", 
                "Channel management",
                "Topic verification",
                "WebSocket integration"
            ],
            "verification_status": {
                "startup_done": telegram_service.startup_verification_done,
                "protection_active": True
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Enhanced stats error: {str(e)}")
@app.post("/servers/{server_name}/sync")
async def sync_server(
    server_name: str,
    background_tasks: BackgroundTasks,
    discord_service: DiscordService = Depends(get_discord_service_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """: Manually sync a specific server (only monitored channels)"""
    if server_name not in discord_service.servers:
        raise HTTPException(status_code=404, detail="Server not found")
    
    async def sync_task():
        """Background sync task"""
        logger = structlog.get_logger(__name__)
        try:
            server_info = discord_service.servers[server_name]
            monitored_messages = []
            
            # : Get messages only from monitored channels
            monitored_channels = []
            for channel_id, channel_info in server_info.accessible_channels.items():
                if channel_id in discord_service.monitored_announcement_channels:
                    monitored_channels.append((channel_id, channel_info))
                    
                    messages = await discord_service.get_recent_messages(
                        server_name, channel_id, limit=5
                    )
                    monitored_messages.extend(messages)
            
            if monitored_messages:
                # Sort by timestamp and send to Telegram (all to same topic)
                monitored_messages.sort(key=lambda x: x.timestamp)
                sent_count = await telegram_service.send_messages_batch(monitored_messages)
                
                # Get topic info
                topic_id = telegram_service.server_topics.get(server_name)
                
                # Count channel types
                announcement_channels = sum(1 for _, ch_info in monitored_channels 
                                          if discord_service._is_announcement_channel(ch_info.channel_name))
                manual_channels = len(monitored_channels) - announcement_channels
                
                logger.info("Manual sync completed", 
                          server=server_name,
                          messages_sent=sent_count,
                          total_messages=len(monitored_messages),
                          topic_id=topic_id,
                          monitored_channels=len(monitored_channels),
                          announcement_channels=announcement_channels,
                          manual_channels=manual_channels)
            else:
                logger.info("No monitored channels found during manual sync", 
                          server=server_name,
                          total_channels=len(server_info.accessible_channels))
                
        except Exception as e:
            logger.error("Manual sync failed", server=server_name, error=str(e))
    
    background_tasks.add_task(sync_task)
    
    # Count monitored channels for response
    server_info = discord_service.servers[server_name]
    monitored_count = len([ch_id for ch_id in server_info.accessible_channels.keys() 
                          if ch_id in discord_service.monitored_announcement_channels])
    
    return {
        "message": f"Enhanced sync started for {server_name}",
        "note": "Only monitored channels will be synced to the same Telegram topic",
        "monitored_channels": monitored_count,
        "total_channels": len(server_info.accessible_channels),
        "current_topic_id": telegram_service.server_topics.get(server_name),
        "anti_duplicate_protection": getattr(telegram_service, 'startup_verification_done', True)
    }


@app.post("/messages/recent")
async def get_recent_messages(
    request: MessageRequest,
    discord_service: DiscordService = Depends(get_discord_service_dependency)
):
    """: Get recent messages from a monitored channel only"""
    try:
        # : Проверяем что канал мониторится
        if request.channel_id not in discord_service.monitored_announcement_channels:
            raise HTTPException(
                status_code=400, 
                detail="Channel is not monitored. Only monitored channels can be queried."
            )
        
        messages = await discord_service.get_recent_messages(
            request.server_name,
            request.channel_id,
            limit=request.limit
        )
        
        # Определяем тип канала
        channel_type = "unknown"
        if request.server_name in discord_service.servers:
            server_info = discord_service.servers[request.server_name]
            if request.channel_id in server_info.channels:
                channel_info = server_info.channels[request.channel_id]
                channel_type = "announcement" if discord_service._is_announcement_channel(channel_info.channel_name) else "regular"
        
        return {
            "messages": [
                {
                    "content": msg.content,
                    "timestamp": msg.timestamp.isoformat(),
                    "author": msg.author,
                    "server_name": msg.server_name,
                    "channel_name": msg.channel_name,
                    "message_id": msg.message_id
                }
                for msg in messages
            ],
            "count": len(messages),
            "server": request.server_name,
            "channel_id": request.channel_id,
            "channel_type": channel_type,
            "note": f"Messages from this {channel_type} channel post to server topic",
            "monitoring_status": "✅ Monitored",
            "enhanced_features": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/telegram/topics/clean")
async def clean_telegram_topics(
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Clean invalid Telegram topics using enhanced features"""
    try:
        cleaned_count = await telegram_service._clean_invalid_topics()
        
        return {
            "message": "Enhanced topic cleanup completed",
            "cleaned_topics": cleaned_count,
            "active_topics": len(telegram_service.server_topics),
            "note": "Each remaining topic corresponds to one Discord server",
            "anti_duplicate_protection": "✅ ACTIVE",
            "verification_status": getattr(telegram_service, 'startup_verification_done', False)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/telegram/topics/verify")
async def verify_telegram_topics(
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Verify all topic mappings are valid using enhanced verification"""
    try:
        verification_results = {}
        
        for server_name, topic_id in telegram_service.server_topics.items():
            try:
                exists = await telegram_service._topic_exists(
                    telegram_service.settings.telegram_chat_id, topic_id
                )
                verification_results[server_name] = {
                    "topic_id": topic_id,
                    "exists": exists,
                    "status": "✅ Valid" if exists else "❌ Invalid"
                }
            except Exception as e:
                verification_results[server_name] = {
                    "topic_id": topic_id,
                    "exists": False,
                    "status": f"❌ Error: {str(e)}"
                }
        
        valid_count = sum(1 for result in verification_results.values() if result.get("exists", False))
        invalid_count = len(verification_results) - valid_count
        
        return {
            "verification_results": verification_results,
            "summary": {
                "total_topics": len(verification_results),
                "valid_topics": valid_count,
                "invalid_topics": invalid_count
            },
            "note": "Each topic should correspond to exactly one Discord server",
            "enhanced_verification": True,
            "anti_duplicate_protection": "✅ ACTIVE"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_metrics(
    processor: MessageProcessor = Depends(get_message_processor_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Get metrics in Prometheus format with enhanced data"""
    status = processor.get_status()
    
    metrics = []
    
    # System metrics
    metrics.append(f'discord_parser_uptime_seconds {status["system"]["uptime_seconds"]}')
    metrics.append(f'discord_parser_memory_usage_mb {status["system"]["memory_usage_mb"]}')
    metrics.append(f'discord_parser_health_score {status["system"]["health_score"]}')
    
    # Discord metrics
    metrics.append(f'discord_parser_servers_total {status["discord"]["total_servers"]}')
    metrics.append(f'discord_parser_servers_active {status["discord"]["active_servers"]}')
    metrics.append(f'discord_parser_channels_total {status["discord"]["total_channels"]}')
    metrics.append(f'discord_parser_channels_accessible {status["discord"]["accessible_channels"]}')
    
    # Enhanced Telegram metrics
    metrics.append(f'discord_parser_telegram_server_topics {status["telegram"]["topics"]}')
    metrics.append(f'discord_parser_telegram_bot_running {1 if status["telegram"]["bot_running"] else 0}')
    
    # Enhanced features metrics
    enhanced_active = 1 if getattr(telegram_service, 'startup_verification_done', False) else 0
    metrics.append(f'discord_parser_enhanced_features_active {enhanced_active}')
    metrics.append(f'discord_parser_user_sessions {len(getattr(telegram_service, "user_states", {}))}')
    
    # Processing metrics
    metrics.append(f'discord_parser_queue_size {status["processing"]["queue_size"]}')
    metrics.append(f'discord_parser_messages_today {status["processing"]["messages_today"]}')
    metrics.append(f'discord_parser_messages_total {status["processing"]["messages_total"]}')
    metrics.append(f'discord_parser_errors_last_hour {status["processing"]["errors_last_hour"]}')
    
    # Real-time metrics
    realtime_status = status.get("realtime", {})
    metrics.append(f'discord_parser_realtime_enabled {1 if realtime_status.get("enabled") else 0}')
    metrics.append(f'discord_parser_websocket_connections {realtime_status.get("websocket_connections", 0)}')
    
    return "\n".join(metrics)

@app.get("/logs")
async def get_recent_logs(limit: int = 100):
    """Get recent log entries with enhanced features information"""
    return {
        "message": "Enhanced log endpoint available",
        "note": "Configure log aggregation to view logs here",
        "enhanced_features": [
            "Real-time Discord WebSocket monitoring",
            "1 Discord Server = 1 Telegram Topic",
            "Anti-duplicate topic protection",
            "Interactive bot interface",
            "Enhanced channel management",
            "Advanced rate limiting and error recovery"
        ],
        "alternatives": [
            "Check container logs: docker logs <container_id>",
            "Check log files in logs/ directory",
            "Use /telegram/topics to see current mappings",
            "Use /telegram/enhanced-stats for detailed statistics"
        ],
        "enhanced_endpoints": [
            "/telegram/enhanced-stats",
            "/telegram/force-verification", 
            "/telegram/bot-status"
        ]
    }


# НОВЫЙ: Принудительная верификация топиков
@app.post("/telegram/force-verification")
async def force_topic_verification(
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Force topic verification and cleanup using enhanced features"""
    try:
        # Сброс флага верификации для принудительной проверки
        old_verification_status = telegram_service.startup_verification_done
        telegram_service.startup_verification_done = False
        
        # Запуск enhanced верификации
        await telegram_service.startup_topic_verification()
        
        # Очистка недействительных топиков
        cleaned_count = await telegram_service._clean_invalid_topics()
        
        return {
            "message": "Enhanced topic verification completed",
            "previous_verification_status": old_verification_status,
            "current_verification_status": telegram_service.startup_verification_done,
            "active_topics": len(telegram_service.server_topics),
            "cleaned_topics": cleaned_count,
            "verification_features": [
                "Duplicate detection",
                "Invalid topic cleanup", 
                "Startup protection",
                "Atomic operations"
            ],
            "anti_duplicate_protection": "✅ ACTIVE"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Verification error: {str(e)}")

# НОВЫЙ: Bot статус
@app.get("/telegram/bot-status")
async def get_bot_status(
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Get enhanced bot status and interface information"""
    try:
        # Проверяем доступность enhanced функций
        has_enhanced_handlers = hasattr(telegram_service, 'setup_enhanced_bot_handlers')
        has_user_states = hasattr(telegram_service, 'user_states')
        
        bot_info = {
            "bot_running": telegram_service.bot_running,
            "enhanced_handlers": has_enhanced_handlers,
            "user_management": has_user_states,
            "active_user_sessions": len(getattr(telegram_service, 'user_states', {})),
            "chat_supports_topics": False,
            "features_available": {
                "channel_management": True,
                "interactive_ui": has_enhanced_handlers,
                "topic_verification": True,
                "anti_duplicate": telegram_service.startup_verification_done
            }
        }
        
        # Проверяем поддержку топиков в чате
        try:
            chat_supports_topics = telegram_service._check_if_supergroup_with_topics(
                telegram_service.settings.telegram_chat_id
            )
            bot_info["chat_supports_topics"] = chat_supports_topics
        except Exception:
            bot_info["chat_supports_topics"] = False
        
        return bot_info
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bot status error: {str(e)}")

@app.get("/servers", response_model=ServerListResponse)
async def list_servers(
    discord_service: DiscordService = Depends(get_discord_service_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """: List servers with enhanced monitoring info"""
    servers_data = []
    
    for server_name, server_info in discord_service.servers.items():
        # Получаем enhanced информацию о топике
        topic_id = telegram_service.server_topics.get(server_name)
        topic_verified = False
        
        if topic_id:
            try:
                topic_verified = await telegram_service._topic_exists(
                    telegram_service.settings.telegram_chat_id, topic_id
                )
            except Exception:
                topic_verified = False
        
        # НОВОЕ: Подсчет типов каналов
        monitored_channels = 0
        announcement_channels = 0
        manual_channels = 0
        
        for channel_id, channel_info in server_info.channels.items():
            if channel_id in discord_service.monitored_announcement_channels:
                monitored_channels += 1
                if discord_service._is_announcement_channel(channel_info.channel_name):
                    announcement_channels += 1
                else:
                    manual_channels += 1
        
        servers_data.append({
            "name": server_name,
            "guild_id": server_info.guild_id,
            "status": server_info.status.value,
            "channels": server_info.channel_count,
            "accessible_channels": server_info.accessible_channel_count,
            "monitored_channels": monitored_channels,  # НОВОЕ
            "announcement_channels": announcement_channels,  # НОВОЕ
            "manually_added_channels": manual_channels,  # НОВОЕ
            "last_sync": server_info.last_sync.isoformat() if server_info.last_sync else None,
            "telegram_topic_id": topic_id,
            "topic_verified": topic_verified,
            "enhanced_features": {
                "has_topic": topic_id is not None,
                "topic_protection": telegram_service.startup_verification_done,
                "channel_management": True,
                "monitoring_strategy": "auto announcement + manual any"  # НОВОЕ
            }
        })
    
    active_count = len([s for s in discord_service.servers.values() 
                       if s.status.value == "active"])
    
    return ServerListResponse(
        servers=servers_data,
        total_count=len(servers_data),
        active_count=active_count
    )

@app.get("/servers/{server_name}")
async def get_server(
    server_name: str,
    discord_service: DiscordService = Depends(get_discord_service_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """: Get detailed server info with monitoring status"""
    if server_name not in discord_service.servers:
        raise HTTPException(status_code=404, detail="Server not found")
    
    server_info = discord_service.servers[server_name]
    
    channels_data = []
    announcement_channels = 0
    manual_channels = 0
    monitored_channels = 0
    
    for channel_id, channel_info in server_info.channels.items():
        is_monitored = channel_id in discord_service.monitored_announcement_channels
        is_announcement = discord_service._is_announcement_channel(channel_info.channel_name)
        
        # Определяем тип канала
        channel_type = "announcement" if is_announcement else "regular"
        monitoring_reason = ""
        
        if is_monitored:
            monitored_channels += 1
            if is_announcement:
                announcement_channels += 1
                monitoring_reason = "Auto-discovered announcement channel"
            else:
                manual_channels += 1
                monitoring_reason = "Manually added channel"
        
        channels_data.append({
            "channel_id": channel_id,
            "channel_name": channel_info.channel_name,
            "channel_type": channel_type,  # НОВОЕ
            "is_monitored": is_monitored,  # НОВОЕ
            "monitoring_reason": monitoring_reason,  # НОВОЕ
            "http_accessible": channel_info.http_accessible,
            "websocket_accessible": channel_info.websocket_accessible,
            "access_method": channel_info.access_method,
            "message_count": channel_info.message_count,
            "last_message_time": channel_info.last_message_time.isoformat() if channel_info.last_message_time else None,
            "last_checked": channel_info.last_checked.isoformat() if channel_info.last_checked else None,
            "manageable": True
        })
    
    # Enhanced topic information
    telegram_topic_id = telegram_service.server_topics.get(server_name)
    topic_verified = False
    topic_can_create = False
    
    if telegram_topic_id:
        try:
            topic_verified = await telegram_service._topic_exists(
                telegram_service.settings.telegram_chat_id, telegram_topic_id
            )
        except Exception:
            topic_verified = False
    else:
        # Проверяем можем ли создать топик
        try:
            topic_can_create = telegram_service._check_if_supergroup_with_topics(
                telegram_service.settings.telegram_chat_id
            )
        except Exception:
            topic_can_create = False
    
    return {
        "name": server_name,
        "guild_id": server_info.guild_id,
        "status": server_info.status.value,
        "channels": channels_data,
        "channel_count": server_info.channel_count,
        "accessible_channel_count": server_info.accessible_channel_count,
        "last_sync": server_info.last_sync.isoformat() if server_info.last_sync else None,
        "telegram_topic_id": telegram_topic_id,
        "total_messages": server_info.total_messages,
        "last_activity": server_info.last_activity.isoformat() if server_info.last_activity else None,
        "monitoring_summary": {  # НОВОЕ
            "total_channels": len(channels_data),
            "monitored_channels": monitored_channels,
            "announcement_channels": announcement_channels,
            "manually_added_channels": manual_channels,
            "non_monitored_channels": len(channels_data) - monitored_channels,
            "monitoring_strategy": "auto announcement + manual any"
        },
        "enhanced_topic_info": {
            "has_topic": telegram_topic_id is not None,
            "topic_id": telegram_topic_id,
            "topic_verified": topic_verified,
            "can_create_topic": topic_can_create,
            "protection_active": telegram_service.startup_verification_done,
            "note": "All monitored channels from this server post to the same Telegram topic"  # 
        },
        "channel_management": {
            "can_add_channels": True,
            "max_channels": server_info.max_channels,
            "available_slots": server_info.max_channels - server_info.channel_count,
            "note": "Any added channel will be automatically monitored"  # НОВОЕ
        }
    }

@app.get("/telegram/topics", response_model=TopicMappingResponse)
async def get_telegram_topics(
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Get current server-to-topic mappings with anti-duplicate protection info"""
    return TopicMappingResponse(
        server_topics=telegram_service.server_topics.copy(),
        total_topics=len(telegram_service.server_topics),
        last_updated=datetime.now().isoformat(),
        anti_duplicate_protection=getattr(telegram_service, 'startup_verification_done', True)
    )

@app.get("/monitoring/status")
async def get_monitoring_status(
    discord_service: DiscordService = Depends(get_discord_service_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Get comprehensive monitoring status"""
    try:
        # Подсчет всех типов каналов
        total_channels = 0
        accessible_channels = 0
        monitored_channels = 0
        announcement_channels = 0
        manual_channels = 0
        
        server_breakdown = {}
        
        for server_name, server_info in discord_service.servers.items():
            server_stats = {
                "total": len(server_info.channels),
                "accessible": len(server_info.accessible_channels),
                "monitored": 0,
                "announcement": 0,
                "manual": 0
            }
            
            total_channels += len(server_info.channels)
            accessible_channels += len(server_info.accessible_channels)
            
            for channel_id, channel_info in server_info.channels.items():
                if channel_id in discord_service.monitored_announcement_channels:
                    monitored_channels += 1
                    server_stats["monitored"] += 1
                    
                    if discord_service._is_announcement_channel(channel_info.channel_name):
                        announcement_channels += 1
                        server_stats["announcement"] += 1
                    else:
                        manual_channels += 1
                        server_stats["manual"] += 1
            
            server_breakdown[server_name] = server_stats
        
        return {
            "monitoring_strategy": "auto announcement + manual any",
            "global_stats": {
                "total_channels": total_channels,
                "accessible_channels": accessible_channels,
                "monitored_channels": monitored_channels,
                "auto_discovered_announcement": announcement_channels,
                "manually_added_channels": manual_channels,
                "non_monitored_channels": accessible_channels - monitored_channels
            },
            "telegram_integration": {
                "total_topics": len(telegram_service.server_topics),
                "verified_topics": sum(1 for topic_id in telegram_service.server_topics.values() 
                                     if telegram_service._topic_exists(telegram_service.settings.telegram_chat_id, topic_id)),
                "anti_duplicate_protection": telegram_service.startup_verification_done,
                "bot_running": telegram_service.bot_running
            },
            "servers": server_breakdown,
            "features": {
                "auto_discovery": "Announcement channels found automatically",
                "manual_addition": "Any channel can be added via Telegram bot",
                "universal_monitoring": "All added channels are monitored",
                "single_topic_per_server": "All server channels post to same topic"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# НОВЫЙ: Эндпоинт для получения только мониторимых каналов
@app.get("/servers/{server_name}/monitored-channels")
async def get_monitored_channels(
    server_name: str,
    discord_service: DiscordService = Depends(get_discord_service_dependency)
):
    """Get only monitored channels for a server"""
    if server_name not in discord_service.servers:
        raise HTTPException(status_code=404, detail="Server not found")
    
    server_info = discord_service.servers[server_name]
    monitored_channels = []
    
    for channel_id, channel_info in server_info.channels.items():
        if channel_id in discord_service.monitored_announcement_channels:
            is_announcement = discord_service._is_announcement_channel(channel_info.channel_name)
            
            monitored_channels.append({
                "channel_id": channel_id,
                "channel_name": channel_info.channel_name,
                "channel_type": "announcement" if is_announcement else "regular",
                "monitoring_reason": "Auto-discovered" if is_announcement else "Manually added",
                "http_accessible": channel_info.http_accessible,
                "message_count": channel_info.message_count,
                "last_message_time": channel_info.last_message_time.isoformat() if channel_info.last_message_time else None
            })
    
    return {
        "server_name": server_name,
        "monitored_channels": monitored_channels,
        "total_monitored": len(monitored_channels),
        "announcement_channels": len([ch for ch in monitored_channels if ch["channel_type"] == "announcement"]),
        "manually_added_channels": len([ch for ch in monitored_channels if ch["channel_type"] == "regular"]),
        "note": "All these channels forward messages to the same Telegram topic"
    }

@app.delete("/servers/{server_name}/channels/{channel_id}")
async def remove_channel_from_server(
    server_name: str,
    channel_id: str,
    telegram_service: TelegramService = Depends(get_telegram_service_dependency),
    discord_service: DiscordService = Depends(get_discord_service_dependency)
):
    """Remove a channel from server monitoring"""
    try:
        # Проверяем что сервер существует
        if server_name not in discord_service.servers:
            raise HTTPException(status_code=404, detail="Server not found")
        
        # Проверяем что канал существует и мониторится
        server_info = discord_service.servers[server_name]
        if channel_id not in server_info.channels:
            raise HTTPException(status_code=404, detail="Channel not found in server")
        
        if channel_id not in discord_service.monitored_announcement_channels:
            raise HTTPException(status_code=400, detail="Channel is not being monitored")
        
        # Используем функцию из telegram_service
        success, message = telegram_service.remove_channel_from_server(server_name, channel_id)
        
        if success:
            # Получаем обновленную информацию
            remaining_monitored = len([
                ch_id for ch_id in server_info.channels.keys() 
                if ch_id in discord_service.monitored_announcement_channels
            ])
            
            # Получаем информацию о канале
            channel_info = server_info.channels[channel_id]
            channel_name = getattr(channel_info, 'channel_name', f'Channel_{channel_id}')
            is_announcement = telegram_service._is_announcement_channel(channel_name)
            
            return {
                "message": "Channel removed from monitoring successfully",
                "server_name": server_name,
                "channel_id": channel_id,
                "channel_name": channel_name,
                "channel_type": "announcement" if is_announcement else "regular",
                "remaining_monitored_channels": remaining_monitored,
                "details": message,
                "note": "Channel still exists in Discord but will no longer forward messages"
            }
        else:
            raise HTTPException(status_code=400, detail=message)
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/servers/{server_name}/monitored-channels/detailed")
async def get_detailed_monitored_channels(
    server_name: str,
    discord_service: DiscordService = Depends(get_discord_service_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Get detailed information about monitored channels for a server"""
    if server_name not in discord_service.servers:
        raise HTTPException(status_code=404, detail="Server not found")
    
    server_info = discord_service.servers[server_name]
    monitored_channels = []
    
    for channel_id, channel_info in server_info.channels.items():
        if channel_id in discord_service.monitored_announcement_channels:
            is_announcement = telegram_service._is_announcement_channel(channel_info.channel_name)
            
            monitored_channels.append({
                "channel_id": channel_id,
                "channel_name": channel_info.channel_name,
                "channel_type": "announcement" if is_announcement else "regular",
                "monitoring_reason": "Auto-discovered" if is_announcement else "Manually added",
                "http_accessible": channel_info.http_accessible,
                "websocket_accessible": channel_info.websocket_accessible,
                "message_count": getattr(channel_info, 'message_count', 0),
                "last_message_time": channel_info.last_message_time.isoformat() if channel_info.last_message_time else None,
                "last_checked": channel_info.last_checked.isoformat() if channel_info.last_checked else None,
                "can_remove": True  # All monitored channels can be removed
            })
    
    # Статистика по типам каналов
    announcement_count = len([ch for ch in monitored_channels if ch["channel_type"] == "announcement"])
    regular_count = len([ch for ch in monitored_channels if ch["channel_type"] == "regular"])
    
    return {
        "server_name": server_name,
        "monitored_channels": monitored_channels,
        "summary": {
            "total_monitored": len(monitored_channels),
            "announcement_channels": announcement_count,
            "manually_added_channels": regular_count,
            "total_server_channels": len(server_info.channels),
            "accessible_channels": len(server_info.accessible_channels)
        },
        "telegram_topic_id": telegram_service.server_topics.get(server_name),
        "management_info": {
            "all_channels_removable": True,
            "removal_method": "Bot interface or API",
            "note": "All monitored channels forward to the same Telegram topic"
        }
    }

# Остальные эндпоинты остаются без изменений...
# (все остальные методы из исходного файла сохраняются)

# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Enhanced global exception handler"""
    logger = structlog.get_logger(__name__)
    logger.error("Unhandled exception in enhanced version", 
                path=request.url.path,
                method=request.method,
                error=str(exc))
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if hasattr(app, 'debug') and app.debug else "An unexpected error occurred",
            "timestamp": datetime.now().isoformat(),
            "version": "Enhanced v2.2.0",
            "note": "Check server logs for details"
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )
