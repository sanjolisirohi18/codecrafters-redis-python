from typing import List, Any, Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

class RedisType(Enum):
    STRING = "string"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    HASH = "hash"
    STREAM = "stream"
    VECTORSET = "vectorset"
@dataclass
class RedisValue:
    value: Any
    type: RedisType = RedisType.STRING
    start_time: datetime = field(default_factory=datetime.now)
    options: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RedisResponse:
    """ A simple container for the encoded bytes to send back to the client. """
    payload: bytes

    def to_bytes(self) -> bytes:
        return self.payload
class RedisRequest:
    def __init__(self, command = None, data = None):
        self.command: str = command
        self.data: List[str] = data
    
    @classmethod
    def parse_from_buffer(cls, buffer: bytearray) -> Tuple[Optional['RedisRequest'], int]:
        """
        Attempts to parse a full RESP array from the byte buffer.
        Returns (RedisRequest, bytes_consumed) if successful.
        Returns (None, 0) if the buffer is incomplete.
        """

        # Basic RESP Array check: *<count>\r\n
        if not buffer.startswith(b"*"):
            return None, 0
        
        lines: List[bytes] = buffer.split(b"\r\n")
        if len(lines) < 2:
            return None, 0
        
        try:
            num_elements: int = int(lines[0][1:])
        except (ValueError, IndexError):
            return None, 0
        
        # Each element in a Redis command array is a Bulk String: $<len>\r\n<data>\r\n
        # So for N elements, we need 2 lines per element + the header line.
        # Header: *N\r\n (1 line)
        # Elements: $L\r\nDATA\r\n (2 lines per element)
        expected_lines: int = 1 + (num_elements * 2)

        if len(lines) <= expected_lines:
            return None, 0
        
        # Extract Values
        actual_values: List[str] = []
        bytes_consumed: int = len(lines[0]) + 2 # Start with header length + \r\n
        print(f"num_elements: {num_elements}")
        print(f"type of num_elements: {type(num_elements)}")

        for i in range(num_elements):
            # Indexing: line 1 is $len, line 2 is data, line3 is $len...
            val: str = lines[2 + (i*2)].decode('utf-8')
            actual_values.append(val)

            # Keep track of exactly how many bytes we've "read"
            bytes_consumed += len(lines[1 + (i * 2)]) + 2 # the $<len>\r\n part
            bytes_consumed += len(lines[2 + (i * 2)]) + 2 # the DATA\r\n part
        
        command: str = actual_values[0].lower()
        data: List[str] = actual_values[1:]

        return cls(command=command, data=data), bytes_consumed
    
    # @classmethod
    # def from_raw_data(cls, raw_data:str) -> 'RedisRequest':
    #     data = raw_data.split("\r\n")
    #     print(f"data: {data}")

    #     if not data:
    #         return cls("")

    #     actual_values: List[str] = data[2::2]
    #     print(f"actual_values: {actual_values}")

    #     if not actual_values:
    #         return cls("")
        
    #     command: str = actual_values[0].lower()
    #     command_data: List[str] = actual_values[1:]

    #     print(f"command: {command}")
    #     print(f"command data: {command_data}")

    #     return cls(command=command, data=command_data)

# class RedisResponse:
#     """Build and format the response sent back to the client. """
#     def __init__(self, response = None, length = None, command = None, error = None):
#         self.response: Any = response
#         self.length: str = length
#         self.command: str = command
#         self.error: str = error
    
#     def simple_string_response(self) -> str:
#         """ Generate simple string response. """
#         if self.error:
#             return f"-{self.error}\r\n"
        
#         return f"+{self.response}\r\n"
    
#     def bulk_string_response(self) -> str:
#         """ Generare bulk string response. """

#         if self.response is None:
#             return f"$-1\r\n"
        
#         return f"${self.length}\r\n{self.response}\r\n"
    
#     def integer_response(self) -> str:
#         """ Generate integer response. """
#         return f":{self.length}\r\n"
    
#     def array_response(self) -> str:
#         """ Generate array response. """

#         if self.response is None:
#             return f"*-1\r\n"

#         result: List[str] = []

#         for value in self.response:
#             result.append(f"${len(value)}")
#             result.append("\r\n")
#             result.append(value)
#             result.append("\r\n")

#         return f"*{self.length}\r\n{"".join(result)}"

#     def to_bytes(self) -> bytes:
#         """ Generates the final formatted response. """

#         if self.command == "lpop":
#             if isinstance(self.response, str):
#                 return self.bulk_string_response().encode()
#             else:
#                 return self.array_response().encode()

#         if self.command in {"ping", "set", "type"}:
#             return self.simple_string_response().encode()
        
#         if self.command in {"get", "xadd"}:
#             if self.error:
#                 return self.simple_string_response().encode()
            
#             return self.bulk_string_response().encode()
        
#         if self.command in {"rpush", "lpush", "llen"}:
#             return self.integer_response().encode()
        
#         if self.command in {"lrange", "blpop"}:
#             return self.array_response().encode()
        
#         return self.bulk_string_response().encode()

