from datetime import datetime, timedelta
from typing import List

from .models import RedisRequest, RedisResponse, RedisValue

DATA_STORE = {}

# Handler Functions

def handle_ping_command(request: RedisRequest) -> RedisResponse:
    """ Handler for PING command. """
    return RedisResponse(response="PONG", command=request.command)

def handle_echo_command(request: RedisRequest) -> RedisResponse:
    """ Hanlder for ECHO command. """
    print(f"Redis command: {request.command}")
    print(f"Redis data: {request.data}")

    response: str = request.data[0]

    return RedisResponse(response=response, length=f"{len(response)}", command=request.command)

def handle_set_command(request: RedisRequest) -> RedisResponse:
    """ 
    Handler for SET command. 
    Stores a value in DATA_STORE
    """
    # Data Structure: [key_len, key, value_len, value]
    key: str = request.data[0]
    value: str = request.data[1]

    options_dict: dict = {}

    for i in range(2, len(request.data)-1, 2):
        options_dict[request.data[i].upper()] = int(request.data[i+1])
    
    
    DATA_STORE[key] = RedisValue(
        value=value,
        options=options_dict
    )
    print(f"DATA_STORE: {DATA_STORE}")

    return RedisResponse(response="OK", command=request.command)

def handle_get_command(request: RedisRequest) -> RedisRequest:
    """ 
    Handler for GET command. 
    Retrieves data from DATA_STORE
    """
    curr_time: datetime = datetime.now()
    key: str = request.data[0]
    redis_value: str = DATA_STORE.get(key, None)
    print(f"value: {redis_value}")

    if redis_value is None:
        return RedisResponse(response=None, command=request.command)

    if 'PX' in redis_value.options:
        if curr_time > redis_value.start_time + timedelta(milliseconds=int(redis_value.options["PX"])):
            return RedisResponse(response=None, command=request.command)

    return RedisResponse(response=redis_value.value, length=f"{len(redis_value.value)}", command=request.command)

def handle_rpush_command(request: RedisRequest) -> RedisResponse:
    """ Handler for RPUSH command. """

    key: str = request.data[0]

    if key not in DATA_STORE:
        DATA_STORE[key] = []
    
    values: List[str] = request.data[1:]

    for value in values:
        DATA_STORE[key].append(value)
    
    return RedisResponse(response=None, length=f"{len(DATA_STORE[key])}", command=request.command)
    
