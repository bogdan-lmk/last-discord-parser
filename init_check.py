import asyncio
from app.services.discord_service import DiscordService
from app.config import Settings
from app.utils.rate_limiter import RateLimiter

async def main():
    settings = Settings()
    rate_limiter = RateLimiter()
    discord = DiscordService(settings, rate_limiter)
    if await discord.initialize():
        print(f'Initialized servers: {len(discord.servers)}')
    else:
        print('Failed to initialize DiscordService')

if __name__ == '__main__':
    asyncio.run(main())
