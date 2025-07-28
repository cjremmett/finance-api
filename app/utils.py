import redis
import httpx
import asyncio
import time
import sqlalchemy
REDIS_HOST = '192.168.0.121'
BASE_URL = 'https://cjremmett.com/logging'


def get_redis_cursor(host='localhost', port=6379):
    return redis.Redis(host, port, db=0, decode_responses=True)


def get_secrets_dict():
    r = get_redis_cursor(host=REDIS_HOST)
    secrets_list = r.json().get('secrets', '$')
    return secrets_list[0]


def get_logging_microservice_token() -> str:
    return get_secrets_dict()['secrets']['logging_microservice']['api_token']


def get_finance_token() -> str:
    return get_secrets_dict()['secrets']['finance_tools']['api_token']


def authorized_via_finance_token(finance_token:str) -> bool:
    try:
        if finance_token != None and finance_token != '' and finance_token == get_finance_token():
            return True
        else:
            return False
    except Exception as e:
        asyncio.create_task(append_to_log('WARNING', 'Exception thrown in authorization check: ' + repr(e)))
        return False
    

def get_api_key(service: str) -> str:
   try:
        secrets = get_secrets_dict()
        return secrets['secrets']['api_keys'][service]
   except Exception as e:
      asyncio.create_task(append_to_log('WARNING', 'Exception thrown in get_api_key: ' + repr(e)))
      return 'KEY_NOT_FOUND'


async def append_to_log(level: str, message: str) -> None:
    json = {'table': 'cjremmett_logs', 'category': 'FINANCE', 'level': level, 'message': message}
    headers = {'token': get_logging_microservice_token()}
    async with httpx.AsyncClient() as client:
        await client.post(BASE_URL + '/append-to-log', json=json, headers=headers)
        

async def log_resource_access(url: str, ip: str) -> None:
    json = {'resource': url, 'ip_address': ip}
    headers = {'token': get_logging_microservice_token()}
    async with httpx.AsyncClient() as client:
        await client.post(BASE_URL + '/log-resource-access', json=json, headers=headers)


def get_postgres_engine(database):
   try:
      # Postgres is not port forwarded so hardcoded login should be fine
      return sqlalchemy.create_engine("postgresql+psycopg2://admin:pass@192.168.0.121:5432/" + database)
   except Exception as e:
      print('Getting Postgres engine failed. Error:' + repr(e))
      raise Exception('Failed to get SQLAlchemy Postgres engine.')
   

def get_postgres_cursor_autocommit(database):
   return get_postgres_engine(database).connect().execution_options(isolation_level="AUTOCOMMIT")


def get_epoch_time():
   return str(time.time())


def get_calendar_datetime_utc_string():
   return time.datetime.now(time.timezone.utc).strftime('%m/%d/%y %H:%M:%S')


def get_postgres_timestamp_now() -> str:
   # Use this function to get the timestamp string everywhere to ensure the format is consistent across functions and tables
   return time.datetime.now(time.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def get_postgres_date_now() -> str:
   # Use this function to get the date string everywhere to ensure the format is consistent across functions and tables
   # Use for date data type in Postgres
   # e.g. 2024-06-15
   return time.datetime.now(time.timezone.utc).strftime('%Y-%m-%d')


def execute_postgres_query(query: str) -> None:
   try:
      with get_postgres_cursor_autocommit('cjremmett') as cursor:
         cursor.execute(get_sqlalchemy_query_text(query))
   except Exception as e:
      append_to_log('ERROR', 'Exception thrown running the following SQL query:\n\n' + query + '\n\nError:' + repr(e))


def get_sqlalchemy_query_text(query: str) -> sqlalchemy.sql.elements.TextClause:
   try:
      return sqlalchemy.text(query)
   except Exception as e:
      append_to_log('ERROR', repr(e))