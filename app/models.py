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
    
    def ping_command_response(self) -> str:
        """ Generate respone for ping command. """
        return f"+{self.response}\r\n"
    
    def echo_command_response(self) -> str:
        """ Generate a response for echo command. """
        return f"{self.length}\r\n{self.response}\r\n"

    def to_bytes(self) -> bytes:
        """ Generates the final formatted response. """

        if self.command == "ping":
            return self.ping_command_response().encode()
        else:
            return self.echo_command_response().encode()

