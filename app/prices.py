from fastapi import APIRouter, Response, Query, Request, status, Header
from utils import append_to_log, authorized_via_finance_token, get_secrets_dict, get_api_key
import httpx
import asyncio
import re
from typing import Annotated

MONGO_CONNECTION_STRING = 'mongodb://admin:admin@192.168.0.121'

router = APIRouter()


async def get_fx_conversion_rate_from_alpha_vantage(currency: str) -> str:
    # JSON looks like this:
    # {'Realtime Currency Exchange Rate': {'1. From_Currency Code': 'USD', '2. From_Currency Name': 'United States Dollar', 
    # '3. To_Currency Code': 'JPY', '4. To_Currency Name': 'Japanese Yen', '5. Exchange Rate': '155.53900000', 
    # '6. Last Refreshed': '2025-01-21 15:20:01', '7. Time Zone': 'UTC', '8. Bid Price': '155.53250000', '9. Ask Price': '155.54310000'}}
    try:
        api_key = get_api_key('alpha_vantage')
        url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=USD&to_currency={currency}&apikey={api_key}'
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        resp_json = response.json()
        fx_rate = float(resp_json['Realtime Currency Exchange Rate']['5. Exchange Rate'])
        fx_rate = round(fx_rate, 2)
        return str(fx_rate)
    
    except Exception as e:
        await append_to_log('ERROR', f'Failed to get forex conversion rate from Alpha Vantage for currency {currency}. Error: {repr(e)}')
        return None
    

@router.get("/get-forex-conversion", status_code=200)
async def get_fx_rate_to_usd(response: Response, currency: str = Query(..., description="Currency ticker"), token: Annotated[str | None, Header()] = None):
    try:
        if not authorized_via_finance_token(token):
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return {}
        
        # Match a-zA-Z to prevent user from passing bad ticker.
        if currency == None or len(currency) < 1 or len(currency) > 4 or not re.match("^[a-zA-Z]+$", currency):
            await append_to_log('ERROR', 'Bad ticker submitted. Ticker: ' + str(currency))
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {}
        
        # Get the FX conversion rate using Alpha Vantage API
        fx_rate = await get_fx_conversion_rate_from_alpha_vantage(currency)

        # Return the result.
        # VBA has trouble with JSON so just send straight text back since the use case for this is displaying data in Excel.
        if fx_rate != None:
            await append_to_log('TRACE', 'Got forex conversion rate successfully for currency ' + currency + '. Forex conversion rate: ' + fx_rate)
            return(fx_rate)
        else:
            append_to_log('ERROR', 'Failed to get forex conversion successfully for currency ' + currency + '.')
            return('')

    except Exception as e:
        await append_to_log('ERROR', f'Exception thrown in get_fx_rate_to_usd: {repr(e)}')
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {}


async def get_gurufocus_html_source(ticker: str) -> str:
    # As of 3/1/24, GuruFocus has minimal anti-scraping measures.
    # Merely changing the user agent is enough to bypass them.
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
        url = f'https://www.gurufocus.com/stock/{ticker}/summary'
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
        return str(response.content)
    
    except Exception as e:
        await append_to_log('ERROR', f'Exception thrown in get_gurufocus_html_source: {repr(e)}')
        return None


def get_stock_price_from_gurufocus_html_native_currency(source: str, ticker: str):
    # Return the stock price in the native currency
    # Snippet of source we're using:
    # What is Las Vegas Sands Corp(LVS)\'s  stock price today?\n      </span> <div class="t-caption t-label m-t-sm m-b-md" data-v-00a2281e>\n        The current price of LVS is $51.65.
    # The current price of MIC:SBER is \xe2\x82\xbd292.19.
    try:      
        split = source.split('The current price of ')
        if len(split) < 2:
            # Handle ETF case - the page has different formatting
            # Snippet looks like: 
            # ;aA.pretax_margain=a;aA.price=100.3201;aA.price52whigh=100.67;
            split = source.split('.price=')
            if len(split) < 2:
                return None
            else:
                price = split[1].split(';')[0]
                return str(round(float(price), 2))
        
        split = split[1][0:50].split(' ')
        if len(split) < 3:
            return None

        stock_price = split[2][0:-1]
        for i in reversed(range(0, len(stock_price))):
            if ord(stock_price[i]) != 46 and (ord(stock_price[i]) < 48 or ord(stock_price[i]) > 57):
                return stock_price[i+1:]

        return None

    except Exception as e:
        append_to_log('ERROR', f'Failed to get stock price correctly from GuruFocus HTML source for ticker {ticker}. Error:\n' + repr(e))
        return None
    

def get_market_cap_from_gurufocus_html_native_currency(source: str, ticker: str):
    # Return the market cap in billions in the native currency

    try:
        splits = source.split('Market Cap:')
        if len(splits) < 2:
            return None
        
        splits = splits[1].split('<span ')
        if len(splits) < 2:
            return None
        
        splits = splits[1].split('</span>')
        if len(splits) < 2:
            return None
        
        stock_market_cap_letter = splits[0][-1].upper()

        # Looks like data-v-4e6e2268>HK$ 3.56
        market_cap_str = splits[0][:-1]

        for i in reversed(range(0, len(market_cap_str))):
            if ord(market_cap_str[i]) != 46 and (ord(market_cap_str[i]) < 48 or ord(market_cap_str[i]) > 57):
                market_cap_float = market_cap_str[i+1:]
                break
        stock_market_cap_float = float(market_cap_float)
        
        if stock_market_cap_letter == 'B':
            return str(stock_market_cap_float)
        elif stock_market_cap_letter == 'M':
            return str(round(stock_market_cap_float / 1000, 2))
        elif stock_market_cap_letter == 'T':
            return str(round(stock_market_cap_float * 1000, 2))
        else:
            raise Exception('Unkown letter following market cap.')
        
    except Exception as e:
        append_to_log('ERROR', f'Failed to get market cap correctly from GuruFocus HTML source for ticker {ticker}. Error:\n' + repr(e))
        return None

@router.get("/get-stock-price-and-market-cap-gurufocus", status_code=200)
async def get_stock_price_and_market_cap_gurufocus(response: Response, ticker: str = Query(..., description="Stock ticker"), token: Annotated[str | None, Header()] = None):
    try:
        if not authorized_via_finance_token(token):
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return ''
        
        # Match A-Z, a-z, 1-9 or colon to prevent user from passing bad ticker.
        if ticker == None or len(ticker) < 1 or len(ticker) > 12 or not re.match("^[a-zA-Z0-9:]+$", ticker):
            await append_to_log('ERROR', f'Bad ticker submitted. Ticker: {str(ticker)}')
            response.status_code = status.HTTP_400_BAD_REQUEST
            return ''
        ticker = ticker.upper()
        
        # Get HTML source from GuruFocus
        source = await get_gurufocus_html_source(ticker)
        if source == None or len(source) < 100 or len(source) > 10000000:
            await append_to_log('ERROR', f'Failed to get HTML source correctly from GuruFocus for ticker {ticker}.')
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return ''
        
        # Get the stock price and market cap using HTML source from GuruFocus
        stock_price = get_stock_price_from_gurufocus_html_native_currency(source, ticker)
        market_cap = get_market_cap_from_gurufocus_html_native_currency(source, ticker)

        # Return the result.
        # VBA has trouble with JSON so just send straight text back since the use case for this is displaying data in Excel.
        if stock_price != None and market_cap != None:
            await append_to_log('TRACE', f'Got native currency price and market cap successfully from GuruFocus for ticker {ticker}. Stock price: {stock_price}, Market Cap: {market_cap}')
            return(stock_price + ',' + market_cap)
        # Handle ETFs where they have a price but no market cap
        elif stock_price != None and market_cap == None:
            await append_to_log('TRACE', f'Got native currency price successfully from GuruFocus for ticker {ticker}. Stock price: {stock_price}')
            return(stock_price + ',' + 'N/A')
        else:
            await append_to_log('ERROR', f'Failed to get stock price and market cap successfully for {ticker}. Stock price: {str(stock_price)}, Market Cap: {str(market_cap)}')
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return ''

    except Exception as e:
        await append_to_log('ERROR', repr(e))
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return ''


async def get_price_from_alpha_vantage(ticker: str) -> str:
    # JSON looks like this:
    # {
    #     "Meta Data": {
    #         "1. Information": "Intraday (1min) open, high, low, close prices and volume",
    #         "2. Symbol": "IBM",
    #         "3. Last Refreshed": "2025-08-05 19:59:00",
    #         "4. Interval": "1min",
    #         "5. Output Size": "Compact",
    #         "6. Time Zone": "US/Eastern"
    #     },
    #     "Time Series (1min)": {
    #         "2025-08-05 19:59:00": {
    #             "1. open": "250.9899",
    #             "2. high": "250.9899",
    #             "3. low": "250.9899",
    #             "4. close": "250.9899",
    #             "5. volume": "10"
    #         }
    #     }
    # }
    try:     
        api_key = get_api_key('alpha_vantage')
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol={ticker}&apikey={api_key}'
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        resp_json = response.json()
        print(resp_json)
        price = resp_json['Time Series (1min)'][0][0][0]
        price = round(price, 2)
        return str(price)
    
    except Exception as e:
        await append_to_log('ERROR', f'Failed to get price from Alpha Vantage for ticker {ticker}. Error: {repr(e)}')
        return ''
    

async def get_market_cap_from_alpha_vantage(ticker: str) -> str:
    # JSON looks like this:
    # {
    #     "Symbol": "IBM",
    #     "AssetType": "Common Stock",
    #     "Name": "International Business Machines",
    #     "Description": "International Business Machines Corporation (IBM) is an American multinational technology company headquartered in Armonk, New York, with operations in over 170 countries. The company began in 1911, founded in Endicott, New York, as the Computing-Tabulating-Recording Company (CTR) and was renamed International Business Machines in 1924. IBM is incorporated in New York. IBM produces and sells computer hardware, middleware and software, and provides hosting and consulting services in areas ranging from mainframe computers to nanotechnology. IBM is also a major research organization, holding the record for most annual U.S. patents generated by a business (as of 2020) for 28 consecutive years. Inventions by IBM include the automated teller machine (ATM), the floppy disk, the hard disk drive, the magnetic stripe card, the relational database, the SQL programming language, the UPC barcode, and dynamic random-access memory (DRAM). The IBM mainframe, exemplified by the System/360, was the dominant computing platform during the 1960s and 1970s.",
    #     "CIK": "51143",
    #     "Exchange": "NYSE",
    #     "Currency": "USD",
    #     "Country": "USA",
    #     "Sector": "TECHNOLOGY",
    #     "Industry": "COMPUTER & OFFICE EQUIPMENT",
    #     "Address": "1 NEW ORCHARD ROAD, ARMONK, NY, US",
    #     "OfficialSite": "https://www.ibm.com",
    #     "FiscalYearEnd": "December",
    #     "LatestQuarter": "2025-06-30",
    #     "MarketCapitalization": "233503867000",
    #     "EBITDA": "14183000000",
    #     "PERatio": "40.5",
    #     "PEGRatio": "1.921",
    #     "BookValue": "29.53",
    #     "DividendPerShare": "6.69",
    #     "DividendYield": "0.0265",
    #     "EPS": "6.19",
    #     "RevenuePerShareTTM": "69.07",
    #     "ProfitMargin": "0.0911",
    #     "OperatingMarginTTM": "0.183",
    #     "ReturnOnAssetsTTM": "0.0481",
    #     "ReturnOnEquityTTM": "0.227",
    #     "RevenueTTM": "64040002000",
    #     "GrossProfitTTM": "36868002000",
    #     "DilutedEPSTTM": "6.19",
    #     "QuarterlyEarningsGrowthYOY": "0.177",
    #     "QuarterlyRevenueGrowthYOY": "0.077",
    #     "AnalystTargetPrice": "281.77",
    #     "AnalystRatingStrongBuy": "1",
    #     "AnalystRatingBuy": "8",
    #     "AnalystRatingHold": "9",
    #     "AnalystRatingSell": "2",
    #     "AnalystRatingStrongSell": "1",
    #     "TrailingPE": "40.5",
    #     "ForwardPE": "22.88",
    #     "PriceToSalesRatioTTM": "3.646",
    #     "PriceToBookRatio": "8.49",
    #     "EVToRevenue": "4.463",
    #     "EVToEBITDA": "22.15",
    #     "Beta": "0.677",
    #     "52WeekHigh": "296.16",
    #     "52WeekLow": "181.5",
    #     "50DayMovingAverage": "275.79",
    #     "200DayMovingAverage": "246.41",
    #     "SharesOutstanding": "931519000",
    #     "SharesFloat": "929516000",
    #     "PercentInsiders": "0.119",
    #     "PercentInstitutions": "65.278",
    #     "DividendDate": "2025-09-10",
    #     "ExDividendDate": "2025-08-08"
    # }
    try:
        if('.HK' in ticker):
            return ''
        
        api_key = get_api_key('alpha_vantage')
        url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={api_key}'
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        resp_json = response.json()
        market_cap = resp_json['MarketCapitalization']
        return format_market_cap(market_cap)
    
    except Exception as e:
        await append_to_log('ERROR', f'Failed to get market cap from Alpha Vantage for ticker {ticker}. Error: {repr(e)}')
        return ''


def format_market_cap(market_cap):
    market_cap = float(market_cap)
    if market_cap >= 1_000_000_000_000:
        return f"{market_cap / 1_000_000_000_000:.2f}T"
    elif market_cap >= 1_000_000_000:
        return f"{market_cap / 1_000_000_000:.2f}B"
    elif market_cap >= 1_000_000:
        return f"{market_cap / 1_000_000:.2f}M"
    elif market_cap >= 1_000:
        return f"{market_cap / 1_000:.2f}K"
    else:
        return str(market_cap)