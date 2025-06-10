
from fastapi import APIRouter, Response, Query, Request, status, Header
from utils import append_to_log, authorized_via_finance_token, get_secrets_dict
from pymongo import MongoClient
import httpx
import asyncio
from typing import Annotated

MONGO_CONNECTION_STRING = 'mongodb://admin:admin@192.168.0.121'
router = APIRouter()

def get_api_ninjas_api_key() -> str:
    try:
        secrets_dict = get_secrets_dict()
        return secrets_dict['secrets']['api-ninjas']['api_key']
    except Exception as e:
        asyncio.create_task(append_to_log('ERROR', f"Exception thrown in get_api_ninjas_api_key: {repr(e)}"))
        return ''


def get_earnings_call_transcript_from_db(ticker: str, year: int, quarter: int) -> str:
    """
    Queries MongoDB to check if a record exists with matching ticker, year, and quarter.
    Returns the contents of the transcript field if the record exists, otherwise returns an empty string.

    :param ticker: The stock ticker symbol (e.g., 'GOOGL').
    :param year: The year of the earnings call (e.g., 2027).
    :param quarter: The quarter of the earnings call (e.g., 4).
    :return: The transcript as a string if found, otherwise an empty string.
    """
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client["finance"]
        collection = db["earnings_call_transcripts"]

        # Query the database
        query = {"ticker": ticker, "year": year, "quarter": quarter}
        record = collection.find_one(query)

        # Return the transcript if the record exists
        if record and "transcript" in record:
            return record["transcript"]
        else:
            return ""

    except Exception as e:
        asyncio.create_task(append_to_log('ERROR', f"Error querying MongoDB: {repr(e)}"))
        raise Exception(f"Error querying MongoDB: {repr(e)}")

    finally:
        client.close()


def upsert_earnings_call_transcript(ticker: str, year: int, quarter: int, transcript: str) -> bool:
    """
    Upserts an earnings call transcript record into MongoDB.
    If a record with the same ticker, year, and quarter exists, it updates the transcript field.
    Otherwise, it inserts a new record.

    :param ticker: The stock ticker symbol (e.g., 'GOOGL').
    :param year: The year of the earnings call (e.g., 2027).
    :param quarter: The quarter of the earnings call (e.g., 4).
    :param transcript: The transcript content to store.
    :return: True if the operation is successful, False otherwise.
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client["finance"]
        collection = db["earnings_call_transcripts"]

        # Upsert the record
        query = {"ticker": ticker, "year": year, "quarter": quarter}
        update = {"$set": {"transcript": transcript}}
        result = collection.update_one(query, update, upsert=True)

        # Return True if the operation was successful
        return result.acknowledged

    except Exception as e:
        asyncio.create_task(append_to_log('ERROR', f"Error upserting MongoDB record: {repr(e)}"))
        return False

    finally:
        client.close()


async def get_earnings_call_transcript_from_api_ninjas(ticker: str, year: int, quarter: int) -> str:
    """
    Fetches the earnings call transcript from the API Ninjas service.
    Returns the transcript as a string.

    :param ticker: The stock ticker symbol (e.g., 'GOOGL').
    :param year: The year of the earnings call (e.g., 2027).
    :param quarter: The quarter of the earnings call (e.g., 4).
    :return: The transcript as a string.
    """
    try:
        api_key = get_api_ninjas_api_key()
        api_url = f'https://api.api-ninjas.com/v1/earningstranscript?ticker={ticker}&year={year}&quarter={quarter}'
        headers = {'X-Api-Key': api_key}
        async with httpx.AsyncClient() as client:
            response = await client.get(api_url, headers=headers)
        if response.status_code == httpx.codes.OK:
            data = response.json()
            return data['transcript'] if 'transcript' in data else ""
        else:
            await append_to_log('ERROR', f"Error fetching transcript for {ticker} {year} {quarter} from API Ninjas: {response.status_code} {response.content}")
            return ""
    except Exception as e:
        asyncio.create_task(append_to_log('ERROR', f"Exception fetching transcript for {ticker} {year} {quarter} from API Ninjas: {repr(e)}"))
        return ""
    

async def get_earnings_call_transcript(ticker: str, year: int, quarter: int) -> str:
    """
    Fetches the earnings call transcript for a given ticker, year, and quarter.
    First checks the database for an existing record. If not found, fetches from API Ninjas and stores it in the database.

    :param ticker: The stock ticker symbol (e.g., 'GOOGL').
    :param year: The year of the earnings call (e.g., 2027).
    :param quarter: The quarter of the earnings call (e.g., 4).
    :return: The transcript as a string.
    """
    try:
        ticker = ticker.strip().upper()

        # Check if the transcript exists in the database
        transcript = get_earnings_call_transcript_from_db(ticker, year, quarter)
        
        if not transcript or transcript == "":
            await append_to_log('DEBUG', f"Fetching transcript for {ticker} {year} {quarter} from API Ninjas.")

            # If not found, fetch from API Ninjas
            transcript = await get_earnings_call_transcript_from_api_ninjas(ticker, year, quarter)
            
            # Store the fetched transcript in the database
            upsert_earnings_call_transcript(ticker, year, quarter, transcript)
        else:
            await append_to_log('DEBUG', f"Found transcript for {ticker} {year} {quarter} in MongoDB.")

        return transcript
    
    except Exception as e:
        await append_to_log('ERROR', f"Exception in get_earnings_call_transcript: {repr(e)}")
        return ""
    

@router.get("/get-earnings-call-transcript", status_code=200)
async def get_earnings_call_transcript_endpoint(response: Response, request: Request, ticker: str = Query(..., description="Stock ticker symbol"), year: int = Query(..., description="Year of the earnings call"), quarter: int = Query(..., description="Quarter of the earnings call"), token: Annotated[str | None, Header()] = None):
    try:
        if not authorized_via_finance_token(token):
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return {}
        transcript = await get_earnings_call_transcript(ticker, year, quarter)
        return {"transcript": transcript}    
    except Exception as e:
        await append_to_log('ERROR', f"Exception in get_earnings_call_transcript_endpoint: {repr(e)}")
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {}
    
