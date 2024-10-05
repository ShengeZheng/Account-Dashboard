import time

from pprint import pprint
from dataclasses import dataclass
from typing import Any, Dict

import ccxt
import sqlite3
import asyncio
import logging
from logging.handlers import RotatingFileHandler


LOG_FILE = 'binance_monitor.log'
# 设置日志
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=2)
log_handler.setFormatter(log_formatter)

logger = logging.getLogger('position_monitor')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

def init_exchange(config: Dict[str, Any]) -> ccxt.Exchange:
    exchange_class = getattr(ccxt, config['exchange_id'])
    exchange = exchange_class(config)
    exchange.set_sandbox_mode(config.get('sandbox', False))
    return exchange

def init_db(user):
    conn = sqlite3.connect('trading_data.db')
    c = conn.cursor()
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_total_equity
                 (timestamp INTEGER, equity REAL)''')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {user}_net_positions
                 (timestamp INTEGER, symbol TEXT, contracts REAL)''')   
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect('trading_data.db')

async def update_data(exchange: ccxt.Exchange, user):
    conn = get_db_connection()
    c = conn.cursor()

    timestamp = int(time.time())
    try:
        await exchange.load_markets()
        balance = await exchange.fetch_balance()
        positions = await exchange.fetch_positions()

        net_value = float(balance['USDT']['total'])
        c.execute(f"INSERT INTO {user}_total_equity VALUES (?, ?)", (timestamp, net_value))

        active_positions = {}
        for pos in positions:
            amount = float(pos['contracts'])
            if amount != 0:
                symbol = pos['symbol'].replace(':USDT', '')  # 去除 ":USDT" 后缀
                ticker = await exchange.fetch_ticker(pos['symbol'])
                market_price = ticker['last']

                active_positions[symbol] = {
                    'amount': amount if pos['side'] == 'long' else -amount,
                    'entry_price': float(pos['entryPrice']),
                    'market_price': market_price,
                    'unrealized_pnl': float(pos['unrealizedPnl'])
                }
                c.execute(f"INSERT OR REPLACE INTO {user}_net_positions VALUES (?, ?, ?)", 
                  (timestamp, symbol, amount if pos['side'] == 'long' else -amount))

        logger.info(f"Successfully fetched account info. Net value: {net_value}")
        logger.info(f"Number of active positions: {len(active_positions)}")

    except ccxt.NetworkError as e:
        logger.error(f"Network error when fetching account info: {e}")
    except ccxt.ExchangeError as e:
        logger.error(f"Exchange error when fetching account info: {e}")
    except Exception as e:
        logger.error(f"Unexpected error when fetching account info: {type(e).__name__} - {str(e)}")

    conn.commit()
    conn.close()



