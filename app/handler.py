from .models import RedisRequest, RedisResponse

# Handler Functions

def handle_ping_command(request: RedisRequest) -> RedisResponse:
    """ Handler for PING command. """
    return RedisResponse(response="PONG")

def handle_echo_command(request: RedisRequest) -> RedisResponse:
    """ Hanlder for echo command. """
    print(f"Redis command: {request.command}")
    print(f"Redis data: {request.data}")