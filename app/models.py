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
        if not buffer:
            return None, 0

        # Basic RESP Array check: *<count>\r\n
        # 1. Check for the start of an array
        if not buffer.startswith(b'*'):
            return None, len(buffer)
        
        # 2. Find the first CRLF to get the array length
        first_crlf = buffer.find(b'\r\n')
        if first_crlf == -1:
            return None, 0
        
        try:
            num_elements = int(buffer[1:first_crlf])
        except ValueError:
            return None, len(buffer)
        
        # Extract Values
        cursor = first_crlf + 2
        actual_values= []

        # 3. Parse each Bulk String element
        for _ in range(num_elements):
            # Find the '$' line
            next_crlf = buffer.find(b'\r\n', cursor)
            if next_crlf == -1: return None, 0

            try:
                # Get length of the bulk string: e.g., b'$5' -> 5
                bulk_len = int(buffer[cursor+1:next_crlf])
            except ValueError:
                return None, len(buffer)

            cursor = next_crlf + 2
            
            # Check if we have the full data + the trailing \r\n
            if len(buffer) < cursor + bulk_len + 2:
                return None, 0
            
            # Extract the data and decode to string for the handler
            data_content = buffer[cursor : cursor + bulk_len].decode('utf-8')
            actual_values.append(data_content)
            
            # Move cursor past data and the \r\n
            cursor += bulk_len + 2
        
        if not actual_values:
            return None, cursor
        
        command = actual_values[0].lower()
        command_data = actual_values[1:]

        return cls(command=command, data=command_data), cursor
