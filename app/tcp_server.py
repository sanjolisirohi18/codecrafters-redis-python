import socket
import threading
from typing import Tuple

from .models import RedisRequest
from .router import Router
class TcpServer:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port
    
    def handle_client(self, conn: socket.socket, addr: Tuple[str, int]):
        """ Read, processes and responds to a single client connection. """
        print(f"Accepted connection on {addr}")

        try:
            while True:
                try:
                    raw_data = conn.recv(1024).decode()
                    print("=============================================================")

                    if not raw_data:
                        print(f"No data received from {addr}")

                    request = RedisRequest.from_raw_data(raw_data)
                    print(f"request: {request}")
                    print(f"request command: {request.command}")

                    response = Router(command=request.command).route(request=request)
                    print(f"response: {response}")

                    # 4. Send Response
                    if response:
                        conn.sendall(response.to_bytes())

                except Exception as e:
                    print(f"Error handling client: {e}")
        finally:
            # Close connection when done
            conn.close()
            print(f"Connection with {addr} closed.")
    
    def start(self):
        """ Sets up and run the main server loop. """
        server_address = (self.host, self.port)
        server_socket = socket.create_server(server_address, reuse_port=True)
        print(f"server listening on {server_address}")

        while True:
            try:
                conn, addr = server_socket.accept() # Wait for client
                client_thread = threading.Thread(
                    target= self.handle_client,
                    args= (conn, addr)
                )
                client_thread.start()
            except Exception as e:
                print(f"Error acception connection: {e}")
                break
