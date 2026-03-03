import threading

from datetime import datetime, timedelta
from typing import List
from collections import deque

from .models import RedisRequest, RedisResponse, RedisValue, RedisType

DATA_STORE = {}
DATA_CONDITION = threading.Condition()

# Handler Functions

def get_valid_value(key: str):
    """ Helper to get value from store and check expiration. """

    if key not in DATA_STORE:
        return None
    
    redis_value = DATA_STORE[key]

    if 'PX' in redis_value.options:
        expiry_time: datetime = redis_value.start_time + timedelta(milliseconds=int(redis_value.options["PX"]))

        if datetime.now() > expiry_time:
            #del datetime[key]
            return None
    
    return redis_value

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
    
    with DATA_CONDITION:
        DATA_STORE[key] = RedisValue(
            value=value,
            type= RedisType.STRING,
            options=options_dict
        )
        print(f"DATA_STORE: {DATA_STORE}")

    return RedisResponse(response="OK", command=request.command)

def handle_get_command(request: RedisRequest) -> RedisRequest:
    """ 
    Handler for GET command. 
    Retrieves data from DATA_STORE
    """

    key: str = request.data[0]
    redis_value: str = get_valid_value(key) 
    print(f"value: {redis_value}")

    if redis_value is None:
        return RedisResponse(response=None, command=request.command)

    return RedisResponse(response=redis_value.value, length=f"{len(redis_value.value)}", command=request.command)

def handle_type_command(request: RedisRequest) -> RedisResponse:
    """ Handler for TYPE command. """
    key: str = request.data[0]
    redis_value: str = get_valid_value(key) #DATA_STORE.get(key, None)

    if redis_value is None:
        return RedisResponse(response="none", command=request.command)

    return RedisResponse(response=redis_value.type.value, command=request.command)

def handle_rpush_command(request: RedisRequest) -> RedisResponse:
    """ Handler for RPUSH command. """

    key: str = request.data[0]
    values: List[str] = request.data[1:]

    with DATA_CONDITION:
        redis_value = get_valid_value(key)

        if redis_value is None or redis_value.type != RedisType.LIST:
            redis_value = RedisValue(value=deque([]), type=RedisType.LIST)
            DATA_STORE[key] = redis_value

        for val in values:
            redis_value.value.append(val)

        count: int = len(redis_value.value)
        DATA_CONDITION.notify_all() # Wake up any thread waiting in BLPOP
    
    return RedisResponse(response=None, length=f"{count}", command=request.command)

def handle_lpush_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LPUSH command. """

    key: str = request.data[0]

    with DATA_CONDITION:
        redis_value = get_valid_value(key)

        if redis_value is None:
            redis_value = RedisValue(value=deque([]), type=RedisType.LIST)
            DATA_STORE[key] = redis_value
        
        values: List[str] = request.data[1:]

        for value in values:
            redis_value.value.appendleft(value)
        
        DATA_CONDITION.notify_all()
    
    return RedisResponse(response=None, length=f"{len(redis_value.value)}", command=request.command)

def handle_blpop_command(request: RedisRequest) -> RedisResponse:
    """ Handler for BLPOP command. """

    keys: List[str] = request.data[:-1]
    timeout: float = float(request.data[-1])

    with DATA_CONDITION:
        start_wait = datetime.now()
        while True:
            # Check if any of the keys have data
            for key in keys:
                redis_value = get_valid_value(key)
                if redis_value and len(redis_value.value) > 0 and redis_value.type == RedisType.LIST:
                    element: str = redis_value.value.popleft()

                    # BLOP returns [key, value]
                    return RedisResponse(response=[key, element], length="2", command=request.command)
            
            if timeout > 0:
                elapsed = (datetime.now() - start_wait).total_seconds()
                remaining = timeout - elapsed

                if remaining <= 0:
                    return RedisResponse(response=None, command=request.command)
                
                DATA_CONDITION.wait(timeout=remaining)
            else:
                DATA_CONDITION.wait()

def handle_llen_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LLEN command. """

    key: str = request.data[0]
    redis_value = get_valid_value(key)

    if redis_value is None:
        return RedisResponse(response=[], length='0', command=request.command)
    
    value_length: int = len(redis_value.value)
    
    return RedisResponse(response=None, length=f"{value_length}", command=request.command)

def handle_lpop_command(request: RedisRequest) -> RedisRequest:
    """ Handler for LPOP command. """

    key: str = request.data[0]
    redis_value = get_valid_value(key)

    if redis_value is None:
        return RedisResponse(response=[], length='0', command=request.command)

    result: List[str] = []

    if len(request.data) > 1:
        for i in range(int(request.data[1])):
            result.append(redis_value.value.popleft())
    else:
        element: str = redis_value.value.popleft()

        return RedisResponse(response=element, length=f"{len(element)}", command=request.command)

    return RedisResponse(response=result, length=f"{len(result)}", command=request.command)

def handle_lrange_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LRANGE command. """

    key: str = request.data[0]
    redis_value = get_valid_value(key)

    if redis_value is None:
        return RedisResponse(response=[], length='0', command=request.command)
    
    value_length: int = len(redis_value.value)
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
        result.append(redis_value.value[i])

    return RedisResponse(response=result, length=f"{len(result)}", command=request.command)

def validate_entry_ids(redis_value: RedisValue, sequence_id: str) -> RedisResponse:
    """Validate entry ids for XADD command. """

    if sequence_id == "0-0":
        return RedisResponse(command="xadd", error="ERR The ID specified in XADD must be greater than 0-0")

    if redis_value is None:
        return RedisResponse()
    
    seq_id_split: List[str] = sequence_id.split("-")
    req_ms_time: int = int(seq_id_split[0])
    req_seq_num: int = int(seq_id_split[1])
    
    id: str = redis_value.value[-1][0]
    id_split: List[str] = id.split("-")
    ms_time: int = int(id_split[0])
    seq_num: int = int(id_split[1])

    if ms_time > req_ms_time:
        return RedisResponse(command="xadd", error="ERR The ID specified in XADD is equal or smaller than the target stream top item")
    
    if ms_time == req_ms_time:
        if seq_num >= req_seq_num:
            return RedisResponse(command="xadd", error="ERR The ID specified in XADD is equal or smaller than the target stream top item")
    
    return RedisResponse()

def generate_sequence_numbers(redis_value: RedisValue, sequence_id: str) -> str:
    """ Handle for auto-generating sequence numbers. """

    seq_id_split: List[str] = sequence_id.split("-")
    req_ms_time: int = int(seq_id_split[0])
    if req_ms_time == 0:
        return f"{req_ms_time}-1"
    
    if redis_value is None:
        print(f"{sequence_id[:-1]}0")
        return f"{sequence_id[:-1]}0"
    
    #req_seq_num: int = int(seq_id_split[1])

    id: str = redis_value.value[-1][0]
    id_split: List[str] = id.split("-")
    ms_time: int = int(id_split[0])
    print(f"ms_time: {ms_time}")
    seq_num: int = int(id_split[1])
    print(f"seq_num: {seq_num}")

    if ms_time == req_ms_time:
        return f"{ms_time}-{seq_num+1}"
    
    return sequence_id

def handle_xadd_command(request: RedisRequest) -> RedisResponse:
    """ Handle for XADD command. """

    key: str = request.data[0]
    values: List[str] = request.data[1:]
    redis_value = get_valid_value(key)

    print(f"key: {key}")
    print(f"value: {values}")
    print(f"redis_value: {redis_value}")

    unique_id: str = generate_sequence_numbers(redis_value, values[0])
    print(f"unique id: {unique_id}")
    id_check: RedisResponse = validate_entry_ids(redis_value, unique_id)

    if id_check.error:
        return id_check

    if redis_value is None:
        
        redis_value = RedisValue(
            value=deque([(unique_id, values[1], values[2])]),
            type= RedisType.STREAM
        )
        DATA_STORE[key] = redis_value

        return RedisResponse(response=unique_id, length=len(unique_id), command=request.command)
    
    redis_value.value.append((unique_id, values[1], values[2]))
    return RedisResponse(response=unique_id, length=len(unique_id), command=request.command)
    
    # for idx in range(0, len(values), 3):
    #     print(f"id: {values[idx]}")
    #     print(f"key: {values[idx+1]}")
    #     print(f"value: {values[idx+2]}")
    #     unique_id: str = generate_sequence_numbers(redis_value, values[idx])

    #     redis_value.value.append((unique_id, values[idx+1], values[idx+2]))
    #     return RedisResponse(response=unique_id, length=len(values[idx]), command=request.command)

