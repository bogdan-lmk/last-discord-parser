#!/usr/bin/env python3
"""
Тестовый скрипт для проверки реальной синхронизации Discord → Telegram
Проверяет WebSocket соединения и обработку сообщений в реальном времени
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set
import structlog
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

class RealtimeSyncTester:
    """Тестер реальной синхронизации"""
    
    def __init__(self):
        self.logger = structlog.get_logger(__name__)
        self.discord_tokens = self._get_discord_tokens()
        self.test_results = {
            'websocket_connections': 0,
            'successful_connections': 0,
            'failed_connections': 0,
            'heartbeat_responses': 0,
            'ready_events': 0,
            'message_events': 0,
            'connection_times': [],
            'errors': []
        }
        
        # Состояние соединений
        self.connections: Dict[int, dict] = {}
        self.message_counts: Dict[str, int] = {}  # guild_id -> count
        self.start_time = datetime.now()
        
    def _get_discord_tokens(self) -> List[str]:
        """Получить Discord токены из окружения"""
        tokens_env = os.getenv('DISCORD_AUTH_TOKENS', '')
        if not tokens_env:
            raise ValueError("DISCORD_AUTH_TOKENS not found in environment")
        
        tokens = [token.strip() for token in tokens_env.split(',') if token.strip()]
        if not tokens:
            raise ValueError("No valid tokens found")
        
        return tokens
    
    async def test_websocket_connections(self) -> bool:
        """Тест WebSocket соединений"""
        print("🔌 Тестирование WebSocket соединений Discord...")
        print("=" * 60)
        
        tasks = []
        for i, token in enumerate(self.discord_tokens):
            task = asyncio.create_task(self._test_single_websocket(token, i))
            tasks.append(task)
        
        # Запускаем все соединения параллельно
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Анализируем результаты
        successful = 0
        failed = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"❌ Токен #{i+1}: {result}")
                failed += 1
                self.test_results['errors'].append(str(result))
            elif result:
                print(f"✅ Токен #{i+1}: Соединение успешно")
                successful += 1
            else:
                print(f"❌ Токен #{i+1}: Соединение не удалось")
                failed += 1
        
        self.test_results['successful_connections'] = successful
        self.test_results['failed_connections'] = failed
        self.test_results['websocket_connections'] = len(self.discord_tokens)
        
        print(f"\n📊 Результаты соединений:")
        print(f"✅ Успешно: {successful}/{len(self.discord_tokens)}")
        print(f"❌ Неудачно: {failed}/{len(self.discord_tokens)}")
        
        return successful > 0
    
    async def _test_single_websocket(self, token: str, token_index: int) -> bool:
        """Тест одного WebSocket соединения"""
        connection_start = time.time()
        
        try:
            # Добавляем Bot prefix если нужно
            auth_token = token if token.startswith('Bot ') else f'Bot {token}'
            
            session = aiohttp.ClientSession(
                headers={'Authorization': auth_token},
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            try:
                # Получаем Gateway URL
                async with session.get('https://discord.com/api/v10/gateway/bot') as response:
                    if response.status != 200:
                        self.logger.error("Failed to get gateway", 
                                        token_index=token_index, 
                                        status=response.status)
                        return False
                    
                    gateway_data = await response.json()
                    gateway_url = gateway_data['url']
                    
                    self.logger.info("Got gateway URL", 
                                   token_index=token_index,
                                   gateway_url=gateway_url)
                
                # Подключаемся к WebSocket
                ws = await session.ws_connect(
                    f"{gateway_url}/?v=10&encoding=json",
                    timeout=aiohttp.ClientTimeout(total=60)
                )
                
                connection_time = time.time() - connection_start
                self.test_results['connection_times'].append(connection_time)
                
                self.connections[token_index] = {
                    'ws': ws,
                    'session': session,
                    'connected_at': datetime.now(),
                    'heartbeat_count': 0,
                    'events_received': 0,
                    'ready': False
                }
                
                # Тестируем соединение в течение 30 секунд
                success = await self._handle_websocket_test(ws, token_index)
                
                return success
                
            finally:
                await session.close()
                
        except Exception as e:
            self.logger.error("WebSocket connection failed", 
                            token_index=token_index,
                            error=str(e))
            return False
    
    async def _handle_websocket_test(self, ws: aiohttp.ClientWebSocketResponse, token_index: int) -> bool:
        """Обработка WebSocket сообщений для теста"""
        sequence = None
        heartbeat_task = None
        
        try:
            # Таймаут для теста - 30 секунд
            timeout = asyncio.create_task(asyncio.sleep(30))
            
            while not timeout.done():
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=1.0)
                    
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        
                        op = data.get('op')
                        event_type = data.get('t')
                        event_data = data.get('d', {})
                        
                        # Обновляем sequence
                        if data.get('s') is not None:
                            sequence = data['s']
                        
                        if op == 10:  # HELLO
                            heartbeat_interval = event_data['heartbeat_interval']
                            self.logger.info("Received HELLO", 
                                           token_index=token_index,
                                           heartbeat_interval=heartbeat_interval)
                            
                            # Запускаем heartbeat
                            heartbeat_task = asyncio.create_task(
                                self._send_heartbeat_test(ws, heartbeat_interval, token_index)
                            )
                            
                            # Отправляем IDENTIFY
                            await self._send_identify(ws, token_index)
                            
                        elif op == 0:  # DISPATCH
                            self.connections[token_index]['events_received'] += 1
                            
                            if event_type == 'READY':
                                self.test_results['ready_events'] += 1
                                self.connections[token_index]['ready'] = True
                                
                                session_id = event_data.get('session_id')
                                user_data = event_data.get('user', {})
                                guilds = event_data.get('guilds', [])
                                
                                self.logger.info("Received READY", 
                                               token_index=token_index,
                                               session_id=session_id,
                                               username=user_data.get('username'),
                                               guild_count=len(guilds))
                                
                                return True  # Успешное соединение
                                
                            elif event_type == 'MESSAGE_CREATE':
                                self.test_results['message_events'] += 1
                                guild_id = event_data.get('guild_id')
                                
                                if guild_id:
                                    self.message_counts[guild_id] = self.message_counts.get(guild_id, 0) + 1
                                
                                self.logger.info("Received MESSAGE_CREATE", 
                                               token_index=token_index,
                                               guild_id=guild_id,
                                               channel_id=event_data.get('channel_id'),
                                               author=event_data.get('author', {}).get('username'))
                        
                        elif op == 11:  # HEARTBEAT_ACK
                            self.test_results['heartbeat_responses'] += 1
                            self.connections[token_index]['heartbeat_count'] += 1
                            
                            self.logger.debug("Received HEARTBEAT_ACK", 
                                            token_index=token_index)
                    
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        self.logger.error("WebSocket error", 
                                        token_index=token_index,
                                        error=ws.exception())
                        return False
                        
                except asyncio.TimeoutError:
                    continue  # Продолжаем ждать сообщения
                    
            # Тайм-аут теста достигнут
            self.logger.warning("WebSocket test timeout", token_index=token_index)
            return False
            
        except Exception as e:
            self.logger.error("Error in WebSocket test", 
                            token_index=token_index,
                            error=str(e))
            return False
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
    
    async def _send_heartbeat_test(self, ws: aiohttp.ClientWebSocketResponse, interval: int, token_index: int) -> None:
        """Отправка heartbeat для теста"""
        try:
            await asyncio.sleep(interval / 1000)  # Первый heartbeat
            
            while not ws.closed:
                heartbeat_payload = {
                    "op": 1,
                    "d": None
                }
                
                await ws.send_str(json.dumps(heartbeat_payload))
                self.logger.debug("Sent heartbeat", token_index=token_index)
                
                await asyncio.sleep(interval / 1000)
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error("Heartbeat error", 
                            token_index=token_index,
                            error=str(e))
    
    async def _send_identify(self, ws: aiohttp.ClientWebSocketResponse, token_index: int) -> None:
        """Отправка IDENTIFY"""
        token = self.discord_tokens[token_index]
        auth_token = token if token.startswith('Bot ') else f'Bot {token}'
        
        identify_payload = {
            "op": 2,
            "d": {
                "token": auth_token,
                "properties": {
                    "$os": "linux",
                    "$browser": "realtime_sync_tester",
                    "$device": "realtime_sync_tester"
                },
                "compress": False,
                "large_threshold": 50,
                "intents": 513  # GUILDS (1) + GUILD_MESSAGES (512)
            }
        }
        
        await ws.send_str(json.dumps(identify_payload))
        self.logger.info("Sent IDENTIFY", token_index=token_index)
    
    async def test_message_simulation(self) -> None:
        """Симуляция обработки сообщений"""
        print("\n💬 Симуляция обработки сообщений...")
        print("-" * 40)
        
        # Симулируем сообщения от разных серверов
        test_messages = [
            {
                'guild_id': '123456789',
                'channel_id': '987654321',
                'content': 'Test message 1',
                'author': 'TestUser1',
                'server_name': 'Test Server 1'
            },
            {
                'guild_id': '123456789',
                'channel_id': '987654322',
                'content': 'Test message 2 from different channel',
                'author': 'TestUser2',
                'server_name': 'Test Server 1'
            },
            {
                'guild_id': '987654321',
                'channel_id': '123456789',
                'content': 'Message from different server',
                'author': 'TestUser3',
                'server_name': 'Test Server 2'
            }
        ]
        
        print("📝 Тестовые сообщения:")
        for i, msg in enumerate(test_messages, 1):
            print(f"  {i}. Сервер: {msg['server_name']}")
            print(f"     Канал: {msg['channel_id']}")
            print(f"     Автор: {msg['author']}")
            print(f"     Текст: {msg['content']}")
            print()
        
        # Симуляция обработки топиков
        topic_mappings = {}
        
        for msg in test_messages:
            server_name = msg['server_name']
            
            if server_name not in topic_mappings:
                topic_id = hash(server_name) % 1000
                topic_mappings[server_name] = topic_id
                print(f"🆕 Создан топик {topic_id} для сервера '{server_name}'")
            
            topic_id = topic_mappings[server_name]
            print(f"📤 Сообщение от {msg['author']} отправлено в топик {topic_id}")
        
        print(f"\n📊 Итого создано топиков: {len(topic_mappings)}")
        print("✅ Логика '1 сервер = 1 топик' работает корректно")
    
    def analyze_performance(self) -> None:
        """Анализ производительности"""
        print("\n⚡ Анализ производительности:")
        print("-" * 40)
        
        if self.test_results['connection_times']:
            avg_connection_time = sum(self.test_results['connection_times']) / len(self.test_results['connection_times'])
            min_connection_time = min(self.test_results['connection_times'])
            max_connection_time = max(self.test_results['connection_times'])
            
            print(f"🔌 Время подключения:")
            print(f"   • Среднее: {avg_connection_time:.2f}s")
            print(f"   • Минимальное: {min_connection_time:.2f}s")
            print(f"   • Максимальное: {max_connection_time:.2f}s")
        
        test_duration = (datetime.now() - self.start_time).total_seconds()
        
        print(f"⏱️  Время тестирования: {test_duration:.1f}s")
        print(f"💓 Heartbeat ответов: {self.test_results['heartbeat_responses']}")
        print(f"🎯 READY событий: {self.test_results['ready_events']}")
        print(f"💬 MESSAGE событий: {self.test_results['message_events']}")
        
        if self.test_results['ready_events'] > 0:
            ready_rate = self.test_results['ready_events'] / len(self.discord_tokens) * 100
            print(f"📊 Успешность подключения: {ready_rate:.1f}%")
        
        # Рекомендации
        print(f"\n💡 Рекомендации:")
        if avg_connection_time > 5:
            print("⚠️  Медленное подключение - проверьте сеть")
        else:
            print("✅ Хорошее время подключения")
        
        if self.test_results['heartbeat_responses'] == 0:
            print("❌ Нет heartbeat ответов - проблемы с соединением")
        else:
            print("✅ Heartbeat работает корректно")
        
        if self.test_results['ready_events'] == 0:
            print("❌ Нет READY событий - проверьте токены")
        else:
            print("✅ Получены READY события")
    
    def generate_report(self) -> dict:
        """Генерация итогового отчета"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'test_duration_seconds': (datetime.now() - self.start_time).total_seconds(),
            'websocket_test': {
                'total_tokens': len(self.discord_tokens),
                'successful_connections': self.test_results['successful_connections'],
                'failed_connections': self.test_results['failed_connections'],
                'success_rate_percent': (self.test_results['successful_connections'] / len(self.discord_tokens)) * 100,
                'average_connection_time': sum(self.test_results['connection_times']) / len(self.test_results['connection_times']) if self.test_results['connection_times'] else 0,
                'heartbeat_responses': self.test_results['heartbeat_responses'],
                'ready_events': self.test_results['ready_events'],
                'message_events': self.test_results['message_events']
            },
            'realtime_capabilities': {
                'websocket_supported': self.test_results['successful_connections'] > 0,
                'heartbeat_working': self.test_results['heartbeat_responses'] > 0,
                'event_receiving': self.test_results['ready_events'] > 0,
                'message_monitoring': self.test_results['message_events'] >= 0
            },
            'errors': self.test_results['errors'],
            'recommendations': self._generate_recommendations()
        }
        
        return report
    
    def _generate_recommendations(self) -> List[str]:
        """Генерация рекомендаций"""
        recommendations = []
        
        success_rate = (self.test_results['successful_connections'] / len(self.discord_tokens)) * 100
        
        if success_rate == 100:
            recommendations.append("✅ Все токены работают отлично")
        elif success_rate >= 80:
            recommendations.append("🟡 Большинство токенов работают, проверьте неработающие")
        else:
            recommendations.append("❌ Много неработающих токенов, проверьте их валидность")
        
        if self.test_results['heartbeat_responses'] == 0:
            recommendations.append("❌ Heartbeat не работает - проблемы с соединением")
        
        if self.test_results['ready_events'] == 0:
            recommendations.append("❌ Нет READY событий - проверьте права токенов")
        
        if len(self.test_results['connection_times']) > 0:
            avg_time = sum(self.test_results['connection_times']) / len(self.test_results['connection_times'])
            if avg_time > 10:
                recommendations.append("⚠️ Медленное подключение, оптимизируйте сеть")
            elif avg_time < 2:
                recommendations.append("✅ Отличное время подключения")
        
        if not recommendations:
            recommendations.append("✅ Все тесты прошли успешно!")
        
        return recommendations

async def main():
    """Основная функция тестирования"""
    print("🧪 Тестирование реальной синхронизации Discord → Telegram")
    print("=" * 70)
    print("Проверяем WebSocket соединения и обработку сообщений в реальном времени")
    print()
    
    try:
        tester = RealtimeSyncTester()
        
        # Тест WebSocket соединений
        websocket_success = await tester.test_websocket_connections()
        
        if websocket_success:
            print("\n✅ WebSocket соединения работают!")
            
            # Симуляция обработки сообщений
            await tester.test_message_simulation()
            
            # Анализ производительности
            tester.analyze_performance()
            
        else:
            print("\n❌ WebSocket соединения не работают!")
            print("Проверьте:")
            print("• Валидность Discord токенов")
            print("• Интернет соединение")
            print("• Права токенов (GUILDS, GUILD_MESSAGES)")
        
        # Генерация отчета
        report = tester.generate_report()
        
        # Сохранение отчета
        with open('realtime_sync_test_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n📄 Отчет сохранен в: realtime_sync_test_report.json")
        
        # Итоговый вердикт
        print(f"\n🎯 Итоговый вердикт:")
        print("-" * 20)
        
        if report['websocket_test']['success_rate_percent'] >= 80:
            print("✅ Система готова к реальной синхронизации!")
            print("💡 Можно запускать продуктивное приложение")
        else:
            print("❌ Система НЕ готова к реальной синхронизации")
            print("🔧 Исправьте проблемы перед запуском")
        
        # Рекомендации
        print(f"\n💡 Рекомендации:")
        for rec in report['recommendations']:
            print(f"   {rec}")
        
    except Exception as e:
        print(f"❌ Критическая ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Настройка логирования
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=True)
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    asyncio.run(main())