from apscheduler.schedulers.asyncio import AsyncIOScheduler

from utils.binance import init_exchange, init_db, update_data

from utils.constants import BINANCE_UNI_API_KEY, BINANCE_UNI_SECRET
import asyncio
import ccxt
async def main():

    user = 'zsg'
    exchange = ccxt.binance({
    'apiKey': BINANCE_UNI_API_KEY,
    'secret': BINANCE_UNI_SECRET,
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
    })
    # exchange.http_proxy = {"https": "http://127.0.0.1:7890", "http": "http://127.0.0.1:7890"}
    
    # 初始化数据库表
    await init_db(user)
    
    # 每隔 60 秒更新一次数据
    while True:
        await update_data(exchange, user)
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())

