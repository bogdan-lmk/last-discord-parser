# app/main.py
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
    logger.info("Starting Discord Telegram Parser MVP")
    
    try:
        # Initialize dependency injection container
        container.wire(modules=[__name__])
        
        # Get message processor
        message_processor = container.message_processor()
        
        # Initialize all services
        if await message_processor.initialize():
            logger.info("All services initialized successfully")
            
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
    title="Discord Telegram Parser MVP",
    description="Professional Discord to Telegram message forwarding service",
    version="2.0.0",
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

class StatusResponse(BaseModel):
    system: Dict
    discord: Dict
    telegram: Dict
    processing: Dict
    rate_limiting: Dict

class ServerListResponse(BaseModel):
    servers: List[Dict]
    total_count: int
    active_count: int

class MessageRequest(BaseModel):
    server_name: str
    channel_id: str
    limit: int = 10

# API Routes

@app.get("/")
async def root():
    """Root endpoint with basic info"""
    return {
        "name": "Discord Telegram Parser MVP",
        "version": "2.0.0",
        "status": "running",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check(
    settings: Settings = Depends(get_settings_dependency)
):
    """Health check endpoint"""
    global message_processor
    
    if not message_processor:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    status = message_processor.get_status()
    
    return HealthResponse(
        status=status["system"]["status"],
        timestamp=datetime.now(),
        uptime_seconds=status["system"]["uptime_seconds"],
        health_score=status["system"]["health_score"]
    )

@app.get("/status", response_model=StatusResponse)
async def get_status(
    processor: MessageProcessor = Depends(get_message_processor_dependency)
):
    """Get comprehensive system status"""
    return StatusResponse(**processor.get_status())

@app.get("/servers", response_model=ServerListResponse)
async def list_servers(
    discord_service: DiscordService = Depends(get_discord_service_dependency)
):
    """List all configured Discord servers"""
    servers_data = []
    
    for server_name, server_info in discord_service.servers.items():
        servers_data.append({
            "name": server_name,
            "guild_id": server_info.guild_id,
            "status": server_info.status.value,
            "channels": server_info.channel_count,
            "accessible_channels": server_info.accessible_channel_count,
            "last_sync": server_info.last_sync.isoformat() if server_info.last_sync else None,
            "telegram_topic_id": server_info.telegram_topic_id
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
    discord_service: DiscordService = Depends(get_discord_service_dependency)
):
    """Get detailed information about a specific server"""
    if server_name not in discord_service.servers:
        raise HTTPException(status_code=404, detail="Server not found")
    
    server_info = discord_service.servers[server_name]
    
    channels_data = []
    for channel_id, channel_info in server_info.channels.items():
        channels_data.append({
            "channel_id": channel_id,
            "channel_name": channel_info.channel_name,
            "http_accessible": channel_info.http_accessible,
            "websocket_accessible": channel_info.websocket_accessible,
            "access_method": channel_info.access_method,
            "message_count": channel_info.message_count,
            "last_message_time": channel_info.last_message_time.isoformat() if channel_info.last_message_time else None,
            "last_checked": channel_info.last_checked.isoformat() if channel_info.last_checked else None
        })
    
    return {
        "name": server_name,
        "guild_id": server_info.guild_id,
        "status": server_info.status.value,
        "channels": channels_data,
        "channel_count": server_info.channel_count,
        "accessible_channel_count": server_info.accessible_channel_count,
        "last_sync": server_info.last_sync.isoformat() if server_info.last_sync else None,
        "telegram_topic_id": server_info.telegram_topic_id,
        "total_messages": server_info.total_messages,
        "last_activity": server_info.last_activity.isoformat() if server_info.last_activity else None
    }

@app.post("/servers/{server_name}/sync")
async def sync_server(
    server_name: str,
    background_tasks: BackgroundTasks,
    discord_service: DiscordService = Depends(get_discord_service_dependency),
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Manually sync a specific server"""
    if server_name not in discord_service.servers:
        raise HTTPException(status_code=404, detail="Server not found")
    
    async def sync_task():
        """Background sync task"""
        logger = structlog.get_logger(__name__)
        try:
            server_info = discord_service.servers[server_name]
            all_messages = []
            
            # Get recent messages from all accessible channels
            for channel_id, channel_info in server_info.accessible_channels.items():
                messages = await discord_service.get_recent_messages(
                    server_name, channel_id, limit=5
                )
                all_messages.extend(messages)
            
            if all_messages:
                # Sort and send to Telegram
                all_messages.sort(key=lambda x: x.timestamp)
                sent_count = await telegram_service.send_messages_batch(all_messages)
                
                logger.info("Manual sync completed", 
                          server=server_name,
                          messages_sent=sent_count)
            else:
                logger.info("No messages found during manual sync", server=server_name)
                
        except Exception as e:
            logger.error("Manual sync failed", server=server_name, error=str(e))
    
    background_tasks.add_task(sync_task)
    
    return {"message": f"Sync started for {server_name}"}

@app.post("/messages/recent")
async def get_recent_messages(
    request: MessageRequest,
    discord_service: DiscordService = Depends(get_discord_service_dependency)
):
    """Get recent messages from a specific channel"""
    try:
        messages = await discord_service.get_recent_messages(
            request.server_name,
            request.channel_id,
            limit=request.limit
        )
        
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
            "channel_id": request.channel_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/telegram/topics/clean")
async def clean_telegram_topics(
    telegram_service: TelegramService = Depends(get_telegram_service_dependency)
):
    """Clean invalid Telegram topics"""
    try:
        cleaned_count = await telegram_service._clean_invalid_topics()
        
        return {
            "message": "Topic cleanup completed",
            "cleaned_topics": cleaned_count,
            "active_topics": len(telegram_service.server_topics)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_metrics(
    processor: MessageProcessor = Depends(get_message_processor_dependency)
):
    """Get metrics in Prometheus format"""
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
    
    # Telegram metrics
    metrics.append(f'discord_parser_telegram_topics {status["telegram"]["topics"]}')
    metrics.append(f'discord_parser_telegram_bot_running {1 if status["telegram"]["bot_running"] else 0}')
    
    # Processing metrics
    metrics.append(f'discord_parser_queue_size {status["processing"]["queue_size"]}')
    metrics.append(f'discord_parser_messages_today {status["processing"]["messages_today"]}')
    metrics.append(f'discord_parser_messages_total {status["processing"]["messages_total"]}')
    metrics.append(f'discord_parser_errors_last_hour {status["processing"]["errors_last_hour"]}')
    
    return "\n".join(metrics)

@app.get("/logs")
async def get_recent_logs(limit: int = 100):
    """Get recent log entries (if log aggregation is configured)"""
    # This would require log aggregation setup
    # For now, return placeholder
    return {
        "message": "Log endpoint available",
        "note": "Configure log aggregation to view logs here",
        "alternatives": [
            "Check container logs: docker logs <container_id>",
            "Check log files in logs/ directory"
        ]
    }

# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger = structlog.get_logger(__name__)
    logger.error("Unhandled exception", 
                path=request.url.path,
                method=request.method,
                error=str(exc))
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if app.debug else "An unexpected error occurred",
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # Set to True for development
        log_level="info"
    )