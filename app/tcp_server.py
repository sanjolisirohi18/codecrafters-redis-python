import socket

class TcpServer:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port
    
    def start(self):
        """ Sets up and run the main server loop. """
        server_address = (self.host, self.port)
        server_socket = socket.create_server(server_address, reuse_port=True)
        server_socket.accept() # Wait for client
