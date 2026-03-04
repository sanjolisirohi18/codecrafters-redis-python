from typing import List, Any

class RESPEncoder():
    """ Handles encoding Python objects into Redis Serialozation Protocol (RESP) bytes. """

    @staticmethod
    def simple_string(value: str) -> str:
        """ Generate simple string response. """
        
        return f"+{value}\r\n".encode()
    
    @staticmethod
    def error(message: str) -> bytes:
        return f"-{message}\r\n".encode()
    
    @staticmethod
    def bulk_string(value: str | None) -> str:
        """ Generare bulk string response. """

        if value is None:
            return f"$-1\r\n".encode()
        
        return f"${len(value)}\r\n{value}\r\n".encode()
    
    @staticmethod
    def integer(value: int) -> str:
        """ Generate integer response. """
        return f":{value}\r\n".encode()
    
    @staticmethod
    def array(values: List[str] | None) -> str:
        """ Generate array response. """
        print(f"array_values: {values}")

        if values is None:
            return f"*-1\r\n".encode()
        
        encoded_elements: List[bytes] = [RESPEncoder.bulk_string(v) for v in values]
        header: bytes = f"*{len(values)}\r\n".encode()

        return header + b"".join(encoded_elements)