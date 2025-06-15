# app/models/message.py
from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Optional, Dict, Any
import re
import signal
import time
from contextlib import contextmanager

@contextmanager
def timeout_regex(seconds: int = 1):
    """Context manager для таймаута regex операций"""
    def timeout_handler(signum, frame):
        raise TimeoutError("Regex operation timeout")
    
    # Сохраняем старый обработчик
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

def safe_regex_sub(pattern: str, replacement: str, text: str, timeout_seconds: int = 1) -> str:
    """Безопасная замена regex с таймаутом"""
    try:
        with timeout_regex(timeout_seconds):
            return re.sub(pattern, replacement, text)
    except TimeoutError:
        # Fallback: простая замена строк без regex
        if pattern == r'<@!?\d+>':
            # Упрощенная замена для user mentions
            result = text
            for prefix in ['<@!', '<@']:
                while prefix in result:
                    start = result.find(prefix)
                    if start == -1:
                        break
                    end = result.find('>', start)
                    if end == -1:
                        break
                    result = result[:start] + replacement + result[end+1:]
            return result
        elif pattern == r'<#\d+>':
            # Упрощенная замена для channel mentions
            result = text
            while '<#' in result:
                start = result.find('<#')
                if start == -1:
                    break
                end = result.find('>', start)
                if end == -1:
                    break
                result = result[:start] + replacement + result[end+1:]
            return result
        elif pattern == r'<@&\d+>':
            # Упрощенная замена для role mentions
            result = text
            while '<@&' in result:
                start = result.find('<@&')
                if start == -1:
                    break
                end = result.find('>', start)
                if end == -1:
                    break
                result = result[:start] + replacement + result[end+1:]
            return result
        else:
            # Для других паттернов просто возвращаем исходный текст
            return text
    except Exception:
        # При любой другой ошибке возвращаем исходный текст
        return text

class DiscordMessage(BaseModel):
    """Typed Discord message model with validation"""
    
    content: str = Field(..., min_length=1, max_length=4000)
    timestamp: datetime
    server_name: str = Field(..., min_length=1, max_length=100)
    channel_name: str = Field(..., min_length=1, max_length=100)
    author: str = Field(..., min_length=1, max_length=50)
    
    # Optional fields
    message_id: Optional[str] = None
    channel_id: Optional[str] = None
    guild_id: Optional[str] = None
    translated_content: Optional[str] = None
    attachments: Optional[list] = Field(default_factory=list)
    embeds: Optional[list] = Field(default_factory=list)
    
    # Processing metadata
    processed_at: Optional[datetime] = None
    telegram_message_id: Optional[int] = None
    
    @validator('content', pre=True)
    def clean_content(cls, v):
        """Clean and sanitize message content with ReDoS protection"""
        if not v:
            raise ValueError('Message content cannot be empty')
        
        # ИСПРАВЛЕНИЕ: Используем защищенные regex операции
        try:
            # Remove Discord mentions and clean formatting с таймаутом
            v = safe_regex_sub(r'<@!?\d+>', '[User]', v)      # User mentions
            v = safe_regex_sub(r'<#\d+>', '[Channel]', v)      # Channel mentions  
            v = safe_regex_sub(r'<@&\d+>', '[Role]', v)        # Role mentions
        except Exception as e:
            # Если все regex операции не удались, используем простую очистку
            v = v.replace('<@', '[User').replace('<#', '[Channel').replace('<@&', '[Role')
        
        # Trim whitespace
        v = v.strip()
        
        if not v:
            raise ValueError('Message content is empty after cleaning')
        
        return v
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is not in the future"""
        if v > datetime.now():
            raise ValueError('Message timestamp cannot be in the future')
        return v
    
    @validator('server_name', 'channel_name', 'author', pre=True)
    def clean_names(cls, v):
        """Clean server, channel, and author names with safe regex"""
        if not v:
            raise ValueError('Name cannot be empty')
        
        # ИСПРАВЛЕНИЕ: Безопасная очистка имен
        try:
            # Remove problematic characters с таймаутом
            v = safe_regex_sub(r'[^\w\s\-\.]', '', v, timeout_seconds=1)
        except Exception:
            # Fallback: удаляем только самые проблемные символы
            problematic_chars = ['<', '>', '@', '#', '&', '|', '`', '*', '_', '~']
            for char in problematic_chars:
                v = v.replace(char, '')
        
        v = v.strip()
        
        if not v:
            raise ValueError('Name is empty after cleaning')
        
        return v
    
    def to_telegram_format(self, show_timestamp: bool = True, show_server: bool = True) -> str:
        """Format message for Telegram"""
        parts = []
        
        if show_server:
            parts.append(f"🏰 **{self.server_name}**")
        
        parts.append(f"📢 #{self.channel_name}")
        
        if show_timestamp:
            parts.append(f"📅 {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        parts.append(f"👤 {self.author}")
        parts.append(f"💬 {self.content}")
        
        return "\n".join(parts)
    
    class Config:
        # Allow datetime to be set from various formats
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        
        # Example for JSON schema generation
        json_schema_extra = {
            "example": {
                "content": "🎉 New feature released!",
                "timestamp": "2024-01-15T12:00:00",
                "server_name": "My Discord Server",
                "channel_name": "announcements",
                "author": "ServerBot",
                "message_id": "1234567890123456789"
            }
        }