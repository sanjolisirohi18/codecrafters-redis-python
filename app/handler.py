from .models import RedisRequest, RedisResponse

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
    length: str = request.data[0]

    return RedisResponse(response=response, length=length, command=request.command)

def handle_set_command(request: RedisRequest) -> RedisResponse:
    """ 
    Handler for set command. 
    Stores a value in DATA_STORE
    """
    # Data Structure: [key_len, key, value_len, value]
    key = request.data[1]
    value = request.data[3]
    
    DATA_STORE[key] = value

    return RedisResponse(response="OK", command=request.command)

def handle_get_command(request: RedisRequest) -> RedisRequest:
    """ 
    Handler for get command. 
    Retrieves data from DATA_STORE
    """
    pass