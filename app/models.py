from typing import List
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

        #command = data[-2].lower()
        command = data[2].lower()
        command_data = data[3:-1]

        print(f"command: {command}")
        print(f"command data: {command_data}")

        return cls(command=command, data=command_data)

class RedisResponse:
    """Build and format the response sent back to the client. """
    def __init__(self, response = None, length = None, command = None):
        self.response: str = response
        self.length: str = length
        self.command: str = command
    
    def simple_string_response(self) -> str:
        """ Generate simple string response. """
        return f"+{self.response}\r\n"
    
    def bulk_string_response(self) -> str:
        """ Generare bulk string response. """

        if not self.response:
            return f"$-1\r\n"
        
        return f"${self.length}\r\n{self.response}\r\n"
    
    # def ping_command_response(self) -> str:
    #     """ Generate respone for ping command. """
    #     return f"+{self.response}\r\n"
    
    # def echo_command_response(self) -> str:
    #     """ Generate a response for echo command. """
    #     return f"{self.length}\r\n{self.response}\r\n"

    def to_bytes(self) -> bytes:
        """ Generates the final formatted response. """

        if self.command == "ping":
            return self.simple_string_response().encode()
        
        if self.command == "set":
            return self.simple_string_response().encode()
        
        if self.command == "get":
            return self.bulk_string_response().encode()
        
        return self.bulk_string_response().encode()

