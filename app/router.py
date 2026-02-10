from typing import Dict, Callable, List

from .models import RedisResponse, RedisRequest
from .handler import (
    handle_ping_command, 
    handle_echo_command,
    handle_set_command,
)

class Router:
    """ Maps requests to specific handler functions. """
    
    def __init__(self, command = None, data = None):
        self.command: str = command
        self.data: List[str] = data
        self.routes: Dict[str, Callable[[RedisRequest], RedisResponse]] = {
            "ping": handle_ping_command,
            "echo": handle_echo_command,
            "set": handle_set_command,
        }
    
    def route(self, request: RedisRequest) -> RedisResponse:
        """ Dispatches the request to the correct handler based on the path."""

        print(f"command: {request.command}")

        if request.command and request.command in self.routes:
            return self.routes[request.command](request)