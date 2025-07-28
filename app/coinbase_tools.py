from fastapi import APIRouter, Response, Query, Request, status, Header
from utils import append_to_log, authorized_via_finance_token, get_secrets_dict, get_api_key, get_postgres_cursor_autocommit, get_postgres_timestamp_now, get_epoch_time
import time
from typing import Annotated, DefaultDict
from coinbase.websocket import WSClient
import pandas as pd

router = APIRouter()

def get_coinbase_api_credentials() -> tuple:
    secrets_dict = get_secrets_dict()
    return secrets_dict['secrets']['coinbase_api_key']['name'], secrets_dict['secrets']['coinbase_api_key']['privateKey']


def write_crypto_future_data_to_postgres(coin_ticker, spot_bid, spot_ask, perp_bid, perp_ask):
   try:
      with get_postgres_cursor_autocommit('cjremmett') as cursor:
         table = coin_ticker + '_perp_futures'
         log_line = pd.DataFrame({
            'epoch': [get_epoch_time()],
            'timestamp': [get_postgres_timestamp_now()],
            'spot_bid': [spot_bid],
            'spot_ask': [spot_ask],
            'perp_bid': [perp_bid],
            'perp_ask': [perp_ask]
         })
         log_line.to_sql(name=table, con=cursor, if_exists='append', index=False)
   except Exception as e:
      append_to_log('ERROR', 'Writing to coinbase table failed. Error:\n\n' + repr(e))


btc = DefaultDict(float)
eth = DefaultDict(float)
def on_message(msg):
    append_to_log('TRACE', msg)
    if('product_id' in msg):
        if(msg['product_id'] == 'BTC-USD'):
            btc['spot_count'] += 1
            btc['spot_bid'] += msg['best_bid']
            btc['spot_ask'] += msg['best_ask']
        elif(msg['product_id'] == 'ETH-USD'):
            eth['spot_count'] += 1
            eth['spot_bid'] += msg['best_bid']
            eth['spot_ask'] += msg['best_ask']
        elif(msg['product_id'] == 'BTC-PERP-INTX'):
            btc['perp_count'] += 1
            btc['perp_bid'] += msg['best_bid']
            btc['perp_ask'] += msg['best_ask']
        elif(msg['product_id'] == 'ETH-PERP-INTX'):
            eth['perp_count'] += 1
            eth['perp_bid'] += msg['best_bid']
            eth['perp_ask'] += msg['best_ask']


@router.post("/write-crypto-futures-data", status_code=200)
async def write_crypto_futures_data(response: Response, token: Annotated[str | None, Header()] = None):
    try:
        if not authorized_via_finance_token(token):
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return ''
    
        btc = DefaultDict(float)
        eth = DefaultDict(float)
        
        api_credentials = get_coinbase_api_credentials()

        client = WSClient(api_key=api_credentials[0], api_secret=api_credentials[1], on_message=on_message)

        # open the connection and subscribe to the ticker and heartbeat channels for BTC-USD and ETH-USD
        client.open()
        client.subscribe(product_ids=["BTC-PERP-INTX", "BTC-USD", "ETH-PERP-INTX", "ETH-USD"], channels=["ticker"])

        # wait 3 seconds
        time.sleep(3)

        # unsubscribe from the ticker channel and heartbeat channels for BTC-USD and ETH-USD, and close the connection
        client.unsubscribe(product_ids=["BTC-PERP-INTX", "BTC-USD", "ETH-PERP-INTX", "ETH-USD"], channels=["ticker"])
        client.close()

        write_crypto_future_data_to_postgres('btc', btc['spot_bid'] / btc['spot_count'], btc['spot_ask'] / btc['spot_count'], btc['perp_bid'] / btc['perp_count'], btc['perp_ask'] / btc['perp_count'])
        write_crypto_future_data_to_postgres('eth', eth['spot_bid'] / eth['spot_count'], eth['spot_ask'] / eth['spot_count'], eth['perp_bid'] / eth['perp_count'], eth['perp_ask'] / eth['perp_count'])
        
        await append_to_log('DEBUG', 'Finished writing coinbase data to logs.')
        return ''

    except Exception as e:
        await append_to_log('ERROR', repr(e))
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return ''
      
