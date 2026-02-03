class RedisRequest:
    def __init__(self, command: str):
        self.command = command
    
    @classmethod
    def from_raw_data(cls, raw_data:str) -> 'RedisRequest':
        data = raw_data.split("\r\n")
        print(f"data: {data}")
