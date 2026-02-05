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
    def __init__(self, response: str):
        self.response = response
    
    def to_bytes(self) -> bytes:
        """ Generates the final formatted response. """
        # print()
        #response_bytes = self.response.encode()
        print(f"+{self.response}\r\n".encode())

        return f"+{self.response}\r\n".encode()
