from typing import Dict
from datetime import datetime, timedelta
from .models import RedisRequest, RedisResponse, RedisValue

DATA_STORE = {}

# Handler Functions

def handle_ping_command(request: RedisRequest) -> RedisResponse:
    """ Handler for PING command. """
    return RedisResponse(response="PONG", command=request.command)

def handle_echo_command(request: RedisRequest) -> RedisResponse:
    """ Hanlder for echo command. """
    print(f"Redis command: {request.command}")
    print(f"Redis data: {request.data}")

    response: str = request.data[1]

    return RedisResponse(response=response, length=f"{len(response)}", command=request.command)

def handle_set_command(request: RedisRequest) -> RedisResponse:
    """ 
    Handler for set command. 
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
    Handler for get command. 
    Retrieves data from DATA_STORE
    """
    curr_time: datetime = datetime.now()
    key: str = request.data[0]
    value: str = DATA_STORE.get(key, None)
    print(f"value: {value}")

    if value is None:
        return RedisResponse(response=None, command=request.command)

    if 'PX' in value.options:
        if curr_time > value["start_time"] + timedelta(milliseconds=int(value["PX"])):
            return RedisResponse(response=None, command=request.command)

    # if value.get(key_value) is None:
    #     return RedisResponse(response=None, command=request.command)

    return RedisResponse(response=value.value, length=f"{len(value.value)}", command=request.command)