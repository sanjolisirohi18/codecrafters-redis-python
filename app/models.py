class RedisRequest:
    def __init__(self, command: str):
        self.command = command
    
    @classmethod
    def from_raw_data(cls, raw_data:str) -> 'RedisRequest':
        data = raw_data.split("\r\n")
        print(f"data: {data}")

        if not data:
            return cls("")

        command = data[-2]

        return cls(command=command)

class RedisResponse:
    """Build and format the response sent back to the client. """
    def __init__(self, response: str):
        self.response = response
    
    def to_bytes(self) -> bytes:
        """ Generates the final formatted response. """
        # print()
        #response_bytes = self.response.encode()

        return f"+{self.response}\r\n".encode()
