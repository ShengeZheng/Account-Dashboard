from apscheduler.schedulers.asyncio import AsyncIOScheduler

from utils.binance import init_exchange, init_db, update_data

from utils.constants import BINANCE_UNI_API_KEY, BINANCE_UNI_SECRET
import asyncio

config = {
    'exchange_id': 'binance',
    'sandbox': False,
    'apiKey': BINANCE_UNI_API_KEY,
    'secret': BINANCE_UNI_SECRET, 
    'enableRateLimit': False,
    'options': {
        'portfolioMargin': True
    }
}

USER = 'binance1'

if __name__ == '__main__':

    binance = init_exchange(config)
    binance.proxies = {"http":'http://127.0.0.1:7890', "https":"http://127.0.0.1:7890"}
    init_db(USER)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(update_data, 'interval', seconds=15, args=[binance, USER])
    scheduler.start()
    try:
        asyncio.get_event_loop().run_forever()  
    except (KeyboardInterrupt, SystemExit):
        pass
