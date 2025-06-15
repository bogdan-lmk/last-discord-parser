#!/usr/bin/env python3
# setup.py - Установка зависимостей и проверка окружения

import subprocess
import sys
import os
from pathlib import Path

def run_command(command: str, description: str) -> bool:
    """Run shell command and return success status"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} - успешно")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - ошибка:")
        print(f"   {e.stderr}")
        return False

def check_python_version():
    """Check Python version"""
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        print(f"✅ Python версия: {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"❌ Требуется Python 3.8+, найден: {version.major}.{version.minor}.{version.micro}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = ['logs', 'data', 'config']
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"📁 Создана директория: {directory}")

def main():
    """Main setup function"""
    print("🚀 Discord Telegram Parser MVP - Setup")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        return False
    
    # Install dependencies
    if not run_command("pip install -r requirements.txt", "Установка зависимостей"):
        return False
    
    # Create directories
    print("\n📁 Создание директорий...")
    create_directories()
    
    # Check .env file
    if not Path(".env").exists():
        print("\n⚠️ Файл .env не найден")
        print("📝 Создайте .env файл на основе примера:")
        print("   cp .env.example .env")
        print("   # Затем отредактируйте .env с вашими токенами")
    else:
        print("\n✅ Файл .env найден")
        
        # Test configuration
        if run_command("python -m app.debug_config", "Проверка конфигурации"):
            print("\n🎯 Следующие шаги:")
            print("1. Проверьте конфигурацию: python -m app.debug_config")
            print("2. Тестируйте токены: python test_tokens.py")
            print("3. Запустите приложение: python -m app.main")
            return True
    
    return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Установка завершена успешно!")
    else:
        print("\n❌ Установка завершена с ошибками")
    
    sys.exit(0 if success else 1)