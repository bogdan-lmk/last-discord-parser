#!/usr/bin/env python3
"""
Debug Configuration Test Script
Quick test to verify configuration loading works
"""

import os
import sys
from pathlib import Path

# Add the app directory to the Python path
sys.path.insert(0, str(Path(__file__).parent))

def test_config():
    """Test configuration loading"""
    print("🧪 Testing Configuration Loading...")
    print("=" * 50)
    
    try:
        from app.config import Settings, get_settings
        
        print("✅ Config imports successful")
        
        # Test direct instantiation
        print("\n📋 Testing direct Settings instantiation...")
        settings = Settings()
        
        print(f"✅ Settings loaded successfully!")
        print(f"📊 Configuration Summary:")
        print(f"   • App name: {settings.app_name}")
        print(f"   • Debug mode: {settings.debug}")
        print(f"   • Discord tokens: {settings.discord_tokens_count}")
        print(f"   • Telegram chat ID: {settings.telegram_chat_id}")
        print(f"   • Use topics: {settings.use_topics}")
        print(f"   • Max channels per server: {settings.max_channels_per_server}")
        print(f"   • Max total channels: {settings.max_total_channels}")
        print(f"   • Rate limits: Discord {settings.discord_rate_limit_per_second}/s, Telegram {settings.telegram_rate_limit_per_minute}/min")
        
        # Test cached version
        print("\n🔄 Testing cached settings...")
        cached_settings = get_settings()
        print(f"✅ Cached settings loaded: {id(settings) == id(cached_settings)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        # Print more detailed error info
        if hasattr(e, 'errors') and callable(getattr(e, 'errors')):
            print("   Validation errors:")
            for error in e.errors():
                print(f"     • {error.get('loc', ['unknown'])}: {error.get('msg', 'Unknown error')}")
        
        return False

def test_env_vars():
    """Test environment variable loading"""
    print("\n🔍 Environment Variables Check:")
    print("-" * 30)
    
    required_vars = [
        'DISCORD_AUTH_TOKENS',
        'TELEGRAM_BOT_TOKEN', 
        'TELEGRAM_CHAT_ID'
    ]
    
    optional_vars = [
        'DEBUG',
        'LOG_LEVEL',
        'REDIS_URL',
        'TELEGRAM_USE_TOPICS'
    ]
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            if 'TOKEN' in var:
                print(f"✅ {var}: {value[:10]}...{value[-4:]}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NOT SET")
    
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: {value}")
        else:
            print(f"⚪ {var}: not set (using default)")

if __name__ == "__main__":
    print("🔧 Discord Telegram Parser - Configuration Test")
    print("=" * 50)
    
    # Check .env file
    env_path = Path(".env")
    print(f"📁 Working directory: {Path.cwd()}")
    print(f"📄 .env file exists: {env_path.exists()}")
    
    if env_path.exists():
        print(f"📍 .env file path: {env_path.absolute()}")
        
        # Load .env manually for testing
        try:
            from dotenv import load_dotenv
            load_dotenv()
            print("✅ .env file loaded successfully")
        except ImportError:
            print("⚠️  python-dotenv not installed, trying without it...")
    
    # Test environment variables
    test_env_vars()
    
    # Test configuration
    if test_config():
        print("\n🎉 All configuration tests passed!")
        print("\n💡 You can now start the application with:")
        print("   python -m app.main")
        sys.exit(0)
    else:
        print("\n💥 Configuration tests failed!")
        print("\n🔧 Please fix the issues above and try again.")
        sys.exit(1)