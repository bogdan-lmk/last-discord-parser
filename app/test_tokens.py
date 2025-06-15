#!/usr/bin/env python3
# test_tokens.py - Тестирование множественных Discord токенов
"""
Скрипт для тестирования множественных Discord токенов
Usage: python test_tokens.py
"""

import asyncio
import aiohttp
import os
from dotenv import load_dotenv
from datetime import datetime

async def validate_single_token(token: str, token_index: int) -> dict:
    """Validate a single Discord token"""
    result = {
        'index': token_index,
        'token_preview': f"{token[:10]}...{token[-4:]}",
        'valid': False,
        'user_info': None,
        'guild_count': 0,
        'error': None
    }
    
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(
            headers={'Authorization': token},
            timeout=timeout
        ) as session:
            
            # Test basic token validity
            async with session.get('https://discord.com/api/v9/users/@me') as response:
                if response.status != 200:
                    result['error'] = f"HTTP {response.status}"
                    return result
                
                user_data = await response.json()
                result['user_info'] = {
                    'username': user_data.get('username'),
                    'id': user_data.get('id'),
                    'verified': user_data.get('verified', False)
                }
            
            # Test guild access
            async with session.get('https://discord.com/api/v9/users/@me/guilds') as response:
                if response.status == 200:
                    guilds = await response.json()
                    result['guild_count'] = len(guilds)
                else:
                    result['error'] = f"Guild access: HTTP {response.status}"
                    return result
            
            result['valid'] = True
            
    except Exception as e:
        result['error'] = str(e)
    
    return result

async def test_multiple_tokens():
    """Test multiple Discord tokens from environment"""
    print("🔍 Testing Multiple Discord Tokens")
    print("=" * 50)
    
    # Load environment
    load_dotenv()
    
    tokens_env = os.getenv('DISCORD_AUTH_TOKENS')
    if not tokens_env:
        print("❌ DISCORD_AUTH_TOKENS not found in environment")
        return
    
    # Parse tokens
    tokens = [token.strip() for token in tokens_env.split(',') if token.strip()]
    
    if not tokens:
        print("❌ No valid tokens found in DISCORD_AUTH_TOKENS")
        return
    
    print(f"📊 Found {len(tokens)} token(s) to test")
    print()
    
    # Test all tokens concurrently
    print("🧪 Testing tokens...")
    tasks = []
    for i, token in enumerate(tokens):
        task = validate_single_token(token, i)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Analyze results
    valid_tokens = []
    invalid_tokens = []
    total_guilds = 0
    
    print("\n📋 Token Test Results:")
    print("-" * 50)
    
    for result in results:
        if isinstance(result, Exception):
            print(f"❌ Token test failed: {result}")
            continue
        
        token_num = result['index'] + 1
        preview = result['token_preview']
        
        if result['valid']:
            valid_tokens.append(result)
            total_guilds += result['guild_count']
            user_info = result['user_info']
            
            print(f"✅ Token #{token_num}: {preview}")
            print(f"   👤 User: {user_info['username']} (ID: {user_info['id']})")
            print(f"   🏰 Guilds: {result['guild_count']}")
            print(f"   ✉️ Verified: {'Yes' if user_info['verified'] else 'No'}")
        else:
            invalid_tokens.append(result)
            print(f"❌ Token #{token_num}: {preview}")
            print(f"   🚫 Error: {result['error']}")
        
        print()
    
    # Summary
    print("📊 Summary:")
    print("-" * 30)
    print(f"✅ Valid tokens: {len(valid_tokens)}")
    print(f"❌ Invalid tokens: {len(invalid_tokens)}")
    print(f"🏰 Total guilds accessible: {total_guilds}")
    
    if valid_tokens:
        avg_guilds = total_guilds / len(valid_tokens)
        print(f"📈 Average guilds per token: {avg_guilds:.1f}")
        
        # Calculate efficiency rating
        efficiency = calculate_efficiency_rating(len(valid_tokens))
        print(f"⚡ Efficiency rating: {efficiency}")
        
        # Recommendations
        print(f"\n💡 Recommendations:")
        if len(valid_tokens) == 1:
            print("• Single token setup - suitable for small to medium deployments")
            print("• Consider adding more tokens for larger scale or redundancy")
        elif len(valid_tokens) <= 3:
            print("• Good token count for medium deployments")
            print("• Provides load distribution and basic redundancy")
        else:
            print("• Excellent token count for large scale deployments")
            print("• Great load distribution and high redundancy")
        
        print(f"• Recommended max servers: {len(valid_tokens) * 5}")
        print(f"• Recommended max channels: {len(valid_tokens) * 25}")
    
    else:
        print("\n❌ No valid tokens found!")
        print("💡 Please check:")
        print("• Token format and validity")
        print("• Bot permissions and intents")
        print("• Network connectivity")
        print("• Discord API status")

def calculate_efficiency_rating(token_count: int) -> str:
    """Calculate efficiency rating based on token count"""
    if token_count >= 5:
        return "🟢 Excellent (5+ tokens)"
    elif token_count >= 3:
        return "🟡 Good (3-4 tokens)"
    elif token_count >= 2:
        return "🟠 Moderate (2 tokens)"
    else:
        return "🔴 Basic (1 token)"

async def test_load_balancing():
    """Test load balancing distribution"""
    print("\n🔄 Testing Load Balancing:")
    print("-" * 30)
    
    # Load environment
    tokens_env = os.getenv('DISCORD_AUTH_TOKENS')
    if not tokens_env:
        return
    
    tokens = [token.strip() for token in tokens_env.split(',') if token.strip()]
    if len(tokens) < 2:
        print("⚠️ Need at least 2 tokens to test load balancing")
        return
    
    # Simulate server assignments
    test_servers = [
        "Server Alpha", "Server Beta", "Server Gamma", 
        "Server Delta", "Server Epsilon", "Server Zeta",
        "Server Eta", "Server Theta", "Server Iota"
    ]
    
    token_distribution = {i: [] for i in range(len(tokens))}
    
    # Simulate server assignments using consistent hashing
    for server in test_servers:
        token_index = hash(server) % len(tokens)
        token_distribution[token_index].append(server)
    
    print("Token distribution simulation:")
    for token_index, assigned_servers in token_distribution.items():
        print(f"🔑 Token #{token_index + 1}: {len(assigned_servers)} servers")
        for server in assigned_servers:
            print(f"   • {server}")
    
    # Calculate balance
    server_counts = [len(servers) for servers in token_distribution.values()]
    min_servers = min(server_counts)
    max_servers = max(server_counts)
    balance_ratio = min_servers / max_servers if max_servers > 0 else 1.0
    
    print(f"\n📊 Load Balance Analysis:")
    print(f"• Min servers per token: {min_servers}")
    print(f"• Max servers per token: {max_servers}")
    print(f"• Balance ratio: {balance_ratio:.2f}")
    print(f"• Balance quality: {get_balance_quality(balance_ratio)}")

def get_balance_quality(ratio: float) -> str:
    """Get balance quality rating"""
    if ratio >= 0.9:
        return "🟢 Excellent"
    elif ratio >= 0.7:
        return "🟡 Good"
    elif ratio >= 0.5:
        return "🟠 Fair"
    else:
        return "🔴 Poor"

async def main():
    """Main test function"""
    try:
        await test_multiple_tokens()
        await test_load_balancing()
        
        print("\n" + "=" * 50)
        print("✅ Token testing completed successfully!")
        print("💡 Use the results above to optimize your setup")
        
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())