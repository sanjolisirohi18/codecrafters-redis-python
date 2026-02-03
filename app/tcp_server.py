import socket
from typing import Tuple

class TcpServer:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port
    
    def handle_client(self, conn: socket.socket, addr: Tuple[str, int]):
        """ Read, processes and responds to a single client connection. """
        print(f"Accepted connection on {addr}")

        try:
            # Receive data from client
            raw_data = conn.recv(1024).decode()
            print("=============================================================")

            if not raw_data:
                print(f"No data received from {addr}")
            
            print(f"data: {raw_data}")
        except Exception as e:
            print(f"Error handling client")
    
    def start(self):
        """ Sets up and run the main server loop. """
        server_address = (self.host, self.port)
        server_socket = socket.create_server(server_address, reuse_port=True)
        print(f"server listening on {server_address}")

        conn, addr = server_socket.accept() # Wait for client
        self.handle_client(conn=conn, addr=addr)
