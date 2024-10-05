import time
from typing import Any, Dict
import asyncio
import aiosqlite  # 用于异步操作 SQLite 数据库
import ccxt.async_support as ccxt  # 使用 ccxt 的异步版本
import logging
from logging.handlers import RotatingFileHandler
from constants import BINANCE_UNI_API_KEY, BINANCE_UNI_SECRET

# 日志文件路径
LOG_FILE = 'binance_monitor.log'

# 设置日志格式和处理器
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=2)
log_handler.setFormatter(log_formatter)

# 创建日志记录器
logger = logging.getLogger('position_monitor')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)


def init_exchange(api_key: str, secret_key: str) -> ccxt.Exchange:
    """
    使用提供的 API Key 和 Secret 初始化 Binance 交易所实例（异步版本）
    """
    exchange = ccxt.binance({
        'apiKey': api_key,
        'secret': secret_key,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}  # 使用期货市场的设置
    })
    return exchange


async def init_db(user: str):
    """
    初始化数据库表：用于存储总权益和净持仓数据
    """
    async with aiosqlite.connect('trading_data.db') as conn:
        await conn.execute(f'''CREATE TABLE IF NOT EXISTS {user}_total_equity
                               (timestamp INTEGER, equity REAL)''')
        await conn.execute(f'''CREATE TABLE IF NOT EXISTS {user}_net_positions
                               (timestamp INTEGER, symbol TEXT, contracts REAL)''')
        await conn.commit()


async def update_data(exchange: ccxt.Exchange, user: str):
    """
    更新数据并存入数据库
    """
    timestamp = int(time.time())

    async with aiosqlite.connect('trading_data.db') as conn:
        try:
            # 加载市场数据
            await exchange.load_markets()
            
            # 获取账户余额（期货市场）
            balance = await exchange.fetch_balance()
            
            # 获取持仓信息
            positions = await exchange.fetch_positions()
            
            # 获取 USDT 的总权益
            net_value = float(balance['USDT']['total'])
            await conn.execute(f"INSERT INTO {user}_total_equity VALUES (?, ?)", (timestamp, net_value))

            # 记录活跃持仓
            active_positions = {}
            for pos in positions:
                amount = float(pos['contracts'])
                if amount != 0:
                    symbol = pos['symbol'].replace(':USDT', '')  # 去除 ":USDT" 后缀
                    ticker = await exchange.fetch_ticker(pos['symbol'])  # 获取当前市场价格
                    market_price = ticker['last']

                    active_positions[symbol] = {
                        'amount': amount if pos['side'] == 'long' else -amount,
                        'entry_price': float(pos['entryPrice']),
                        'market_price': market_price,
                        'unrealized_pnl': float(pos['unrealizedPnl'])
                    }
                    await conn.execute(f"INSERT OR REPLACE INTO {user}_net_positions VALUES (?, ?, ?)", 
                                       (timestamp, symbol, amount if pos['side'] == 'long' else -amount))

            # 记录日志
            logger.info(f"Successfully fetched account info. Net value: {net_value}")
            logger.info(f"Number of active positions: {len(active_positions)}")

        except ccxt.NetworkError as e:
            logger.error(f"Network error when fetching account info: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error when fetching account info: {e}")
        except Exception as e:
            logger.error(f"Unexpected error when fetching account info: {type(e).__name__} - {str(e)}")

        # 提交数据库操作
        await conn.commit()


async def main():
    """
    主函数，初始化交易所和数据库，并定时更新数据
    """
    user = 'zsg'
    
    # 使用你的 `BINANCE_UNI_API_KEY` 和 `BINANCE_UNI_SECRET` 初始化 Binance 交易所对象
    exchange = init_exchange(api_key=BINANCE_UNI_API_KEY, secret_key=BINANCE_UNI_SECRET)
    
    # 初始化数据库表
    await init_db(user)

    # 每隔 60 秒更新一次数据
    while True:
        await update_data(exchange, user)
        await asyncio.sleep(60)  # 等待 60 秒后再次获取数据


# 异步运行主函数
if __name__ == "__main__":
    asyncio.run(main())