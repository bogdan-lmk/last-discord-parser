# app/models/server.py
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum

class ServerStatus(str, Enum):
    """Server status enumeration"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    PENDING = "pending"

class ChannelInfo(BaseModel):
    """Channel information model"""
    
    model_config = ConfigDict(validate_assignment=True)
    
    channel_id: str = Field(..., pattern=r'^\d{17,19}$')  # Discord snowflake format
    channel_name: str = Field(..., min_length=1, max_length=100)
    category_id: Optional[str] = Field(None, pattern=r'^\d{17,19}$')
    
    # Access tracking
    http_accessible: bool = Field(default=False)
    websocket_accessible: bool = Field(default=False)
    last_checked: Optional[datetime] = None
    
    # Statistics
    message_count: int = Field(default=0, ge=0)
    last_message_time: Optional[datetime] = None
    error_count: int = Field(default=0, ge=0)
    
    @property
    def is_accessible(self) -> bool:
        """Check if channel is accessible via any method"""
        return self.http_accessible or self.websocket_accessible
    
    @property
    def access_method(self) -> str:
        """Get primary access method"""
        if self.http_accessible and self.websocket_accessible:
            return "HTTP+WebSocket"
        elif self.http_accessible:
            return "HTTP only"
        elif self.websocket_accessible:
            return "WebSocket only"
        else:
            return "No access"

class ServerInfo(BaseModel):
    """Discord server information model"""
    
    model_config = ConfigDict(validate_assignment=True)
    
    server_name: str = Field(..., min_length=1, max_length=100)
    guild_id: str = Field(..., pattern=r'^\d{17,19}$')
    
    # Channel management
    channels: Dict[str, ChannelInfo] = Field(default_factory=dict)
    max_channels: int = Field(default=5, ge=1, le=20)
    
    # Status tracking
    status: ServerStatus = Field(default=ServerStatus.PENDING)
    last_sync: Optional[datetime] = None
    error_message: Optional[str] = None
    
    # Telegram integration
    telegram_topic_id: Optional[int] = None
    topic_created_at: Optional[datetime] = None
    
    # Statistics
    total_messages: int = Field(default=0, ge=0)
    active_channels: int = Field(default=0, ge=0)
    last_activity: Optional[datetime] = None
    
    @field_validator('channels')
    @classmethod
    def validate_channel_count(cls, v, info):
        if info.data:
            max_channels = info.data.get('max_channels', 5)
            if len(v) > max_channels:
                raise ValueError(f'Cannot have more than {max_channels} channels per server')
        return v
    
    @property
    def accessible_channels(self) -> Dict[str, ChannelInfo]:
        """Get only accessible channels"""
        return {
            channel_id: channel_info 
            for channel_id, channel_info in self.channels.items()
            if channel_info.is_accessible
        }
    
    @property
    def channel_count(self) -> int:
        """Total number of channels"""
        return len(self.channels)
    
    @property
    def accessible_channel_count(self) -> int:
        """Number of accessible channels"""
        return len(self.accessible_channels)
    
    def add_channel(self, channel_info: ChannelInfo) -> bool:
        """Add a new channel if under limit"""
        if len(self.channels) >= self.max_channels:
            return False
        
        self.channels[channel_info.channel_id] = channel_info
        return True
    
    def remove_channel(self, channel_id: str) -> bool:
        """Remove a channel"""
        if channel_id in self.channels:
            del self.channels[channel_id]
            return True
        return False
    
    def update_stats(self):
        """Update server statistics"""
        self.active_channels = self.accessible_channel_count
        self.last_sync = datetime.now()
        
        if self.active_channels > 0:
            self.status = ServerStatus.ACTIVE
        else:
            self.status = ServerStatus.INACTIVE

class SystemStats(BaseModel):
    """System-wide statistics"""
    
    model_config = ConfigDict(validate_assignment=True)
    
    total_servers: int = Field(default=0, ge=0)
    total_channels: int = Field(default=0, ge=0)
    active_servers: int = Field(default=0, ge=0)
    active_channels: int = Field(default=0, ge=0)
    
    # Message statistics
    messages_processed_today: int = Field(default=0, ge=0)
    messages_processed_total: int = Field(default=0, ge=0)
    
    # Performance metrics
    average_response_time_ms: float = Field(default=0.0, ge=0.0)
    memory_usage_mb: float = Field(default=0.0, ge=0.0)
    uptime_seconds: int = Field(default=0, ge=0)
    
    # Rate limiting
    discord_requests_per_hour: int = Field(default=0, ge=0)
    telegram_requests_per_hour: int = Field(default=0, ge=0)
    
    # Error tracking
    errors_last_hour: int = Field(default=0, ge=0)
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None
    
    @property
    def health_score(self) -> float:
        """Calculate overall health score (0-100)"""
        score = 100.0
        
        # Reduce score for errors
        if self.errors_last_hour > 0:
            score -= min(50, self.errors_last_hour * 5)
        
        # Reduce score for high memory usage
        if self.memory_usage_mb > 1500:  # Above 1.5GB
            score -= 20
        
        # Reduce score for low activity
        if self.active_channels == 0:
            score -= 30
        
        return max(0.0, score)
    
    @property
    def status(self) -> str:
        """Get system status based on health score"""
        health = self.health_score
        if health >= 90:
            return "🟢 Excellent"
        elif health >= 70:
            return "🟡 Good"
        elif health >= 50:
            return "🟠 Warning"
        else:
            return "🔴 Critical"