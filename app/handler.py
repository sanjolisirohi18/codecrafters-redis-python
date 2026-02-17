import threading

from datetime import datetime, timedelta
from typing import List
from collections import deque

from .models import RedisRequest, RedisResponse, RedisValue

DATA_STORE = {}
DATA_CONDITION = threading.Condition()

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

    with DATA_CONDITION:
        if key not in DATA_STORE:
            DATA_STORE[key] = deque([])
        
        values: List[str] = request.data[1:]

        for value in values:
            DATA_STORE[key].append(value)
        
        DATA_CONDITION.notify_all()
    
    return RedisResponse(response=None, length=f"{len(DATA_STORE[key])}", command=request.command)

def handle_lpush_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LPUSH command. """

    key: str = request.data[0]

    with DATA_CONDITION:
        if key not in DATA_STORE:
            DATA_STORE[key] = deque([])
        
        values: List[str] = request.data[1:]

        for value in values:
            DATA_STORE[key].appendleft(value)
        
        DATA_CONDITION.notify_all()
    
    return RedisResponse(response=None, length=f"{len(DATA_STORE[key])}", command=request.command)

def handle_blpop_command(request: RedisRequest) -> RedisResponse:
    """ Handler for BLPOP command. """

    keys: List[str] = request.data[:-1]
    timeout: float = float(request.data[-1])

    end_time: float = datetime.now() + timedelta(seconds=timeout) if timeout > 0 else None

    with DATA_CONDITION:
        while True:
            # Check if any of the keys have data
            for key in keys:
                if key in DATA_STORE and len(DATA_STORE[key]) > 0:
                    element: str = DATA_STORE[key].popleft()

                    # BLOP returns [key, value]
                    return RedisResponse(response=[key, element], length="2", command=request.command)
            
            if timeout > 0:
                remaining: float = (end_time - datetime.now()).total_seconds()

                if remaining <= 0:
                    return RedisResponse(response=None, command=request.command)
                
                DATA_CONDITION.wait(timeout=remaining)
            else:
                DATA_CONDITION.wait()

def handle_llen_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LLEN command. """

    key: str = request.data[0]

    if key not in DATA_STORE:
        return RedisResponse(response=[], length='0', command=request.command)
    
    value_length: int = len(DATA_STORE[key])
    
    return RedisResponse(response=None, length=f"{value_length}", command=request.command)

def handle_lpop_command(request: RedisRequest) -> RedisRequest:
    """ Handler for LPOP command. """

    key: str = request.data[0]

    if key not in DATA_STORE:
        return RedisResponse(response=[], length='0', command=request.command)

    result: List[str] = []

    if len(request.data) > 1:
        for i in range(int(request.data[1])):
            result.append(DATA_STORE[key].popleft())
    else:
        element: str = DATA_STORE[key].popleft()

        return RedisResponse(response=element, length=f"{len(element)}", command=request.command)

    return RedisResponse(response=result, length=f"{len(result)}", command=request.command)

def handle_lrange_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LRANGE command. """

    key: str = request.data[0]

    if key not in DATA_STORE:
        return RedisResponse(response=[], length='0', command=request.command)
    
    value_length: int = len(DATA_STORE[key])
    start_index: int = int(request.data[1]) if int(request.data[1]) >= 0 else value_length + int(request.data[1])
    end_index: int = int(request.data[2]) if int(request.data[2]) >= 0 else value_length + int(request.data[2])

    if start_index < 0:
        start_index = 0

    if start_index > end_index:
        return RedisResponse(response=[], length='0', command=request.command)

    if start_index >= value_length:
        return RedisResponse(response=[], length='0', command=request.command)
    
    if end_index >= value_length:
        end_index = value_length - 1
    
    result: List[str] = []

    for i in range(start_index, end_index+1):
        result.append(DATA_STORE[key][i])

    return RedisResponse(response=result, length=f"{len(result)}", command=request.command)


    
