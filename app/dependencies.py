# app/dependencies.py
from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject
import structlog
import redis
from typing import Optional

from .config import Settings, get_settings
from .services.discord_service import DiscordService
from .services.telegram_service import TelegramService  
from .services.message_processor import MessageProcessor
from .utils.logging import setup_logging
from .utils.rate_limiter import RateLimiter

class Container(containers.DeclarativeContainer):
    """Dependency injection container for the application"""
    
    # Configuration
    config = providers.Singleton(get_settings)
    
    # Logging
    logger = providers.Singleton(
        setup_logging,
        settings=config
    )
    
    # Redis (optional, for caching)
    redis_client = providers.Singleton(
        lambda settings: redis.from_url(settings.redis_url) if settings.redis_url else None,
        settings=config
    )
    
    # Rate Limiters
    discord_rate_limiter = providers.Singleton(
        RateLimiter,
        requests_per_second=config.provided.discord_rate_limit_per_second,
        name="discord"
    )
    
    telegram_rate_limiter = providers.Singleton(
        RateLimiter,
        requests_per_minute=config.provided.telegram_rate_limit_per_minute,
        name="telegram"
    )
    
    # Core Services
    discord_service = providers.Singleton(
        DiscordService,
        settings=config,
        rate_limiter=discord_rate_limiter,
        redis_client=redis_client,
        logger=logger
    )
    
    telegram_service = providers.Singleton(
        TelegramService,
        settings=config,
        rate_limiter=telegram_rate_limiter,
        redis_client=redis_client,
        logger=logger
    )
    
    # Message Processor (orchestrates everything)
    message_processor = providers.Singleton(
        MessageProcessor,
        settings=config,
        discord_service=discord_service,
        telegram_service=telegram_service,
        logger=logger
    )

# Global container instance
container = Container()

# Dependency injection decorators for FastAPI
def get_settings_dependency() -> Settings:
    """FastAPI dependency for settings"""
    return container.config()

def get_logger_dependency():
    """FastAPI dependency for logger"""
    return container.logger()

def get_discord_service_dependency() -> DiscordService:
    """FastAPI dependency for Discord service"""
    return container.discord_service()

def get_telegram_service_dependency() -> TelegramService:
    """FastAPI dependency for Telegram service"""
    return container.telegram_service()

def get_message_processor_dependency() -> MessageProcessor:
    """FastAPI dependency for Message processor"""
    return container.message_processor()