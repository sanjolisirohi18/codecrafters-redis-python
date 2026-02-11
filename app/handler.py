from typing import Dict
from collections import defaultdict
from datetime import datetime, timedelta
from .models import RedisRequest, RedisResponse

# {key: {key_value: "", optional_argument_1: "", start_time: }}
DATA_STORE: Dict[str, Dict[str, str|int]] = defaultdict(dict)

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
    key_dict: Dict = {}
    key_dict["key_value"] = value
    key_dict["start_time"] = datetime.now()

    for i in range(2, len(request.data)-2, 2):
        key_dict[i] = i+1
    
    print(f"key_dict: {key_dict}")
    
    DATA_STORE[key] = key_dict

    return RedisResponse(response="OK", command=request.command)

def handle_get_command(request: RedisRequest) -> RedisRequest:
    """ 
    Handler for get command. 
    Retrieves data from DATA_STORE
    """
    key: str = request.data[0]
    value: str = DATA_STORE.get(key, None)

    if value is None:
        return RedisResponse(response=None, command=request.command)

    return RedisResponse(response=value, length=f"{len(value)}", command=request.command)