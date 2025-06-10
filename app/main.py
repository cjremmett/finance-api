from fastapi import FastAPI, Response, Request
from typing import Callable, Awaitable
from utils import append_to_log, log_resource_access
from fastapi.middleware.cors import CORSMiddleware
import transcripts
import prices

# This is fine because the Mongo port is not port forwarded
MONGO_CONNECTION_STRING = 'mongodb://admin:admin@192.168.0.121'

app = FastAPI()

origins = ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(transcripts.router)
app.include_router(prices.router)

async def log_access(request: Request):
    try:
        url = 'https://cjremmett.com/finance-api/' + str(request.url.path)
        ip_address = request.client.host if request.client else "Unknown"
        await log_resource_access(url, ip_address)
    except Exception as e:
        append_to_log('ERROR', f"Error logging resource access: {repr(e)}")
    
@app.middleware("http")
async def log_all_accesses(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
    await log_access(request)
    response = await call_next(request)
    return response

@app.get("/")
async def heartbeat():
    return {"message": "Finance API is alive!"}