import redis
import httpx
import asyncio
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