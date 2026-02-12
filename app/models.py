from typing import List, Any, Dict
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class RedisValue:
    value: str
    start_time: datetime = field(default_factory=datetime.now)
    options: Dict[str, Any] = field(default_factory=dict)
class RedisRequest:
    def __init__(self, command = None, data = None):
        self.command: str = command
        self.data: List[str] = data
    
    @classmethod
    def from_raw_data(cls, raw_data:str) -> 'RedisRequest':
        data = raw_data.split("\r\n")
        print(f"data: {data}")

        if not data:
            return cls("")

        actual_values: List[str] = data[2::2]
        print(f"actual_values: {actual_values}")

        if not actual_values:
            return cls("")
        
        command: str = actual_values[0].lower()
        command_data: List[str] = actual_values[1:]

        print(f"command: {command}")
        print(f"command data: {command_data}")

        return cls(command=command, data=command_data)

class RedisResponse:
    """Build and format the response sent back to the client. """
    def __init__(self, response = None, length = None, command = None):
        self.response: Any = response
        self.length: str = length
        self.command: str = command
    
    def simple_string_response(self) -> str:
        """ Generate simple string response. """
        return f"+{self.response}\r\n"
    
    def bulk_string_response(self) -> str:
        """ Generare bulk string response. """

        if self.response is None:
            return f"$-1\r\n"
        
        return f"${self.length}\r\n{self.response}\r\n"
    
    def integer_response(self) -> str:
        """ Generate integer response. """
        return f":{self.length}\r\n"
    
    def array_response(self) -> str:
        """ Generate array response. """

        result: List[str] = []

        for value in self.response:
            result.append(f"${len(value)}")
            result.append("\r\n")
            result.append(value)
            result.append("\r\n")

        return f"*{self.length}\r\n{"".join(result)}"

    def to_bytes(self) -> bytes:
        """ Generates the final formatted response. """

        if self.command == "ping":
            return self.simple_string_response().encode()
        
        if self.command == "set":
            return self.simple_string_response().encode()
        
        if self.command == "get":
            print(f"self.response: {self.response}")
            return self.bulk_string_response().encode()
        
        if self.command == "rpush" or self.command == "lpush" or self.command == "llen":
            return self.integer_response().encode()
        
        if self.command == "lrange":
            return self.array_response().encode()
        
        return self.bulk_string_response().encode()

