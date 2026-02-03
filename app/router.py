from typing import Dict, Callable

from .models import RedisResponse, RedisRequest
from .handler import handle_ping_command

class Router:
    """ Maps requests to specific handler functions. """
    
    def __init__(self, command: str):
        self.command = command
        self.routes: Dict[str, Callable[[RedisRequest], RedisResponse]] = {
            "PING": handle_ping_command
        }
    
    def route(self, request: RedisRequest) -> RedisResponse:
        """ Dispatches the request to the correct handler based on the path."""

        print(f"command: {request.command}")

        if request.command and request.command in self.route:
            return self.routes[request.command](request)