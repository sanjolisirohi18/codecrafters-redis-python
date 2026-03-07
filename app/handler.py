import threading

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Any
from collections import deque

from .models import RedisRequest, RedisResponse, RedisValue, RedisType
from .protocols import RESPEncoder

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
    encoded_bytes: bytes = RESPEncoder.simple_string(value="PONG")
    return RedisResponse(payload=encoded_bytes)

def handle_echo_command(request: RedisRequest) -> RedisResponse:
    """ Hanlder for ECHO command. """

    encoded_bytes: bytes = RESPEncoder.bulk_string(value=request.data[0])

    return RedisResponse(payload=encoded_bytes)

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

    encoded_bytes: bytes = RESPEncoder.simple_string(value="OK")
    return RedisResponse(payload=encoded_bytes)

def handle_get_command(request: RedisRequest) -> RedisResponse:
    """ 
    Handler for GET command. 
    Retrieves data from DATA_STORE
    """

    key: str = request.data[0]
    redis_value: str = get_valid_value(key) 

    if redis_value is None:
        return RedisResponse(payload=RESPEncoder.bulk_string(None))

    encoded_bytes: bytes = RESPEncoder.bulk_string(value=redis_value.value)

    return RedisResponse(payload=encoded_bytes)

# ============================================== LISTS HANDLERS ==============================================
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
        encoded_bytes: bytes = RESPEncoder.integer(value=count)
    
    return RedisResponse(payload=encoded_bytes)

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
        encoded_bytes: bytes = RESPEncoder.integer(value=len(redis_value.value))
    
    return RedisResponse(payload=encoded_bytes)

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
                    return RedisResponse(payload=RESPEncoder.array(values=[key, element]))
            
            if timeout > 0:
                elapsed = (datetime.now() - start_wait).total_seconds()
                remaining = timeout - elapsed

                if remaining <= 0:
                    return RedisResponse(payload=RESPEncoder.array(None))
                
                DATA_CONDITION.wait(timeout=remaining)
            else:
                DATA_CONDITION.wait()

def handle_llen_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LLEN command. """

    key: str = request.data[0]
    redis_value = get_valid_value(key)

    if redis_value is None:
        return RedisResponse(payload=RESPEncoder.integer(value=0))
    
    return RedisResponse(payload=RESPEncoder.integer(value=len(redis_value.value)))

def handle_lpop_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LPOP command. """

    key: str = request.data[0]
    redis_value = get_valid_value(key)

    if redis_value is None:
        return RedisResponse(payload=RESPEncoder.array(None))

    result: List[str] = []

    if len(request.data) > 1:
        for i in range(int(request.data[1])):
            result.append(redis_value.value.popleft())
    else:
        element: str = redis_value.value.popleft()

        return RedisResponse(payload=RESPEncoder.bulk_string(value=element))

    return RedisResponse(payload=RESPEncoder.array(values=result))

def handle_lrange_command(request: RedisRequest) -> RedisResponse:
    """ Handler for LRANGE command. """

    key: str = request.data[0]
    redis_value = get_valid_value(key)

    if redis_value is None:
        return RedisResponse(payload=RESPEncoder.array([]))
    
    value_length: int = len(redis_value.value)
    start_index: int = int(request.data[1]) if int(request.data[1]) >= 0 else value_length + int(request.data[1])
    end_index: int = int(request.data[2]) if int(request.data[2]) >= 0 else value_length + int(request.data[2])

    if start_index < 0:
        start_index = 0

    if start_index > end_index:
        return RedisResponse(payload=RESPEncoder.array([]))

    if start_index >= value_length:
        return RedisResponse(payload=RESPEncoder.array([]))
    
    if end_index >= value_length:
        end_index = value_length - 1
    
    result: List[str] = []

    for i in range(start_index, end_index+1):
        result.append(redis_value.value[i])

    return RedisResponse(payload=RESPEncoder.array(values=result))

# ============================================== STREAMS HANDLERS ==============================================

def handle_type_command(request: RedisRequest) -> RedisResponse:
    """ Handler for TYPE command. """
    key: str = request.data[0]
    redis_value: str = get_valid_value(key) #DATA_STORE.get(key, None)
    encoded_bytes: bytes = b""

    if redis_value is None:
        encoded_bytes = RESPEncoder.simple_string(value="none")
    else:
        encoded_bytes = RESPEncoder.simple_string(value=redis_value.type.value)

    return RedisResponse(payload=encoded_bytes)

def id_split(redis_id: str) -> Tuple[int, int]:
    """ 
    Handler to Redis ID split by '-'

    Args:
        redis_id: e.g., 0-1

    Returns:
        Tuple[int,int]: Tuple[timestamp, sequence number]
    """
    values:List[str] = redis_id.split("-")

    return int(values[0]), int(values[1])

def validate_entry_ids(redis_value: RedisValue, sequence_id: str) -> Optional[RedisResponse]:
    """Validate entry ids for XADD command. """

    if sequence_id == "0-0":
        return RedisResponse(payload=RESPEncoder.error(message="ERR The ID specified in XADD must be greater than 0-0"))

    if redis_value is None:
        return None

    req_ms_time, req_seq_num = id_split(sequence_id)
    ms_time, seq_num = id_split(redis_value.value[-1][0])

    if ms_time > req_ms_time:
        return RedisResponse(payload=RESPEncoder.error(message="ERR The ID specified in XADD is equal or smaller than the target stream top item"))
    
    if ms_time == req_ms_time:
        if seq_num >= req_seq_num:
            return RedisResponse(payload=RESPEncoder.error(message="ERR The ID specified in XADD is equal or smaller than the target stream top item"))
    
    return None

def generate_sequence_numbers(redis_value: RedisValue, sequence_id: str) -> str:
    """ Handle for auto-generating sequence numbers. """

    if sequence_id[-1] != "*":
        return sequence_id

    req_ms_time: int = 0

    if sequence_id == "*":
        req_ms_time = int(datetime.now(timezone.utc).replace(tzinfo=None).timestamp() * 1000)
    else:
        seq_id_split: List[str] = sequence_id.split("-")
        req_ms_time: int = int(seq_id_split[0])

    if req_ms_time == 0:
        return f"{req_ms_time}-1"
    
    if redis_value is None:
        return f"{req_ms_time}-0"

    ms_time, seq_num = id_split(redis_value.value[-1][0])

    if ms_time == req_ms_time:
        return f"{ms_time}-{seq_num+1}"
    
    return f"{req_ms_time}-0"

def handle_xadd_command(request: RedisRequest) -> RedisResponse:
    """ Handle for XADD command. """

    key: str = request.data[0]
    values: List[str] = request.data[1:]

    with DATA_CONDITION:
        redis_value = get_valid_value(key)

        unique_id: str = generate_sequence_numbers(redis_value, values[0])
        id_check: RedisResponse = validate_entry_ids(redis_value, unique_id)

        if id_check is not None:
            return id_check

        if redis_value is None:
            
            redis_value = RedisValue(
                value=deque([(unique_id, values[1], values[2])]),
                type= RedisType.STREAM
            )
            DATA_STORE[key] = redis_value

            return RedisResponse(payload=RESPEncoder.bulk_string(value=unique_id))
        else:
            redis_value.value.append((unique_id, values[1], values[2]))

        DATA_CONDITION.notify_all()
    return RedisResponse(payload=RESPEncoder.bulk_string(value=unique_id))

def validate_xrange_id(id: str, type: str = "start") -> str:
    """ Handler to validate start and end IDs for xrange command. """

    for char in id:
        if char == "-":
            return id
    
    return f"{id}-0" if type == "start" else f"{id}-18446744073709551615"

def is_id_in_xrange(redis_id: str, start_id: str, end_id: str) -> bool:
    """ Check if a redis id falls within the given range (inclusive). """
    redis_id_ts, redis_id_seq_num = id_split(redis_id)
    start_ts, start_seq_num = id_split(start_id)
    end_ts, end_seq_num = id_split(end_id)

    # check if before start
    if start_ts > redis_id_ts:
        return False
    
    if start_ts == redis_id_ts and start_seq_num > redis_id_seq_num:
        return False
    
    # Check if after end
    if redis_id_ts > end_ts:
        return False
    
    if redis_id_ts == end_ts and redis_id_seq_num > end_seq_num:
        return False
    
    return True

def encode_stream_entry(entry: Tuple) -> bytes:
    """
    Encode a single stream as RESP. 
    """
    entry_id: str = entry[0]
    fields: List[str] = entry[1:]

    fields_array: List[bytes] = RESPEncoder.array(fields)
    id_encoded: bytes = RESPEncoder.bulk_string(entry_id)
    
    # Manual *2 header since we're combining bulk_string + array
    return b"*2\r\n" + id_encoded + fields_array

def handle_xrange_command(request: RedisRequest) -> RedisResponse:
    """ Handler for XRANGE command. """

    key: str = request.data[0]
    values: List[str] = request.data[1:]
    redis_value = get_valid_value(key)

    if redis_value is None or redis_value.type != RedisType.STREAM:
        return RedisResponse(payload=RESPEncoder.array(None))

    start_id: str = validate_xrange_id(id=values[0], type="start") if values[0] != "-" else redis_value.value[0][0]
    end_id: str = validate_xrange_id(id=values[1], type="end") if values[1] != "+" else f"{redis_value.value[0][0].split("-")[0]}-18446744073709551615"

    matching_entries: List[Any] = []

    for entry in redis_value.value:
        redis_id: str = entry[0]

        if is_id_in_xrange(redis_id, start_id, end_id):
            matching_entries.append(encode_stream_entry(entry))
    
    # Build outer array header manually (entries are pre-encoded bytes)
    header: bytes = f"*{len(matching_entries)}\r\n".encode()
    encoded_bytes: bytes = header + b"".join(matching_entries)

    return RedisResponse(payload=encoded_bytes)

def is_id_in_xread(redis_id: str, start_id: str) -> bool:
    """ Check if a redis id is greater than start_id. """
    redis_id_ts, redis_id_seq_num = id_split(redis_id)
    start_ts, start_seq_num = id_split(start_id)
    
    if redis_id_ts > start_ts:
        return True
    
    if start_ts == redis_id_ts and redis_id_seq_num > start_seq_num:
        return True
    
    return False

def handle_xread_command(request: RedisRequest) -> RedisResponse:
    """ Handler for XREAD command. """
    stream_idx: int = 0

    for idx, value in enumerate(request.data):
        if value == "streams":
            stream_idx = idx
            break
    
    block_timeout: Optional[int] = int(request.data[:stream_idx][1]) if request.data[:stream_idx] else None
    stream_values: List[str] = request.data[stream_idx+1: ]
    num_streams = len(stream_values) // 2
    keys: List[str] = stream_values[:num_streams]
    ids: List[str] = stream_values[num_streams:]

    

    with DATA_CONDITION:
        resolved_ids = []
        for i in range(num_streams):
            key, current_id = keys[i], ids[i]
            if current_id == "$":
                redis_value = get_valid_value(key)
                # Capture current last ID, or "0-0" if empty
                resolved_ids.append(redis_value.value[-1][0] if redis_value and redis_value.value else "0-0")
            else:
                resolved_ids.append(current_id)

        start_wait = datetime.now()
        while True:
            multiple_steam_entries: List[Any] = []

            for idx  in range(num_streams):
                key: str = keys[idx]
                id: str = resolved_ids[idx]
                print(f"id: {id}")

                redis_value = get_valid_value(key)
                
                if redis_value and redis_value.type == RedisType.STREAM:
                    matching_entries: List[Any] = []
                    start_id: str = validate_xrange_id(id=id, type="start")
                    print(f"start_id: {start_id}")

                    for entry in redis_value.value:
                        redis_id: str = entry[0]

                        if is_id_in_xread(redis_id, start_id):
                            matching_entries.append(encode_stream_entry(entry))
                    
                    if matching_entries:
                        header: bytes = f"*{len(matching_entries)}\r\n".encode()
                        single_stream_response: bytes = b"*2\r\n" + RESPEncoder.bulk_string(key) + header + b"".join(matching_entries)
                        multiple_steam_entries.append(single_stream_response)
            
            if multiple_steam_entries:
                encoded_bytes: bytes = f"*{len(multiple_steam_entries)}\r\n".encode() + b"".join(multiple_steam_entries)
                
                return RedisResponse(payload=encoded_bytes)
            
            if block_timeout is None:
                return RedisResponse(payload=RESPEncoder.array(None))

            elapsed = (datetime.now() - start_wait).total_seconds() * 1000
            if block_timeout > 0:
                remaining = block_timeout - elapsed

                if remaining <= 0:
                    return RedisResponse(payload=RESPEncoder.array(None))
                
                DATA_CONDITION.wait(timeout=remaining / 1000)
            else:
                DATA_CONDITION.wait()

# ============================================== TRANSACTIONS HANDLERS ==============================================

def handle_incr_command(request: RedisRequest) -> RedisResponse:
    """ Hanlder for INCR command. """

    key: str = request.data[0]
    # values: List[str] = request.data[1:]
    redis_value = get_valid_value(key)

    if redis_value is None:
        return RedisResponse(payload=RESPEncoder.integer(value=1))

    result: int = int(redis_value.value) + 1

    return RedisResponse(payload=RESPEncoder.integer(value=result))
