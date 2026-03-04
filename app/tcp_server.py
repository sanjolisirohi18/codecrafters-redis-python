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

        # Use a bytearray as a per-connection buffer
        buffer: bytearray = bytearray()

        try:
            while True:
                #try:
                # 1. Read a chunk of raw bytes
                chunk = conn.recv(1024).decode()
                print("=============================================================")

                if not chunk:
                    print(f"No data received from {addr}")
                    break

                buffer.extend(chunk)

                # 2. Parse as many commands as possible
                # This handles the case where multiple commands are sent at once (pipelining)
                while True:
                    request, consumed = RedisRequest.parse_from_buffer(buffer)

                    if request is None:
                        # Buffer doesn't have a full command yet, wait for more recv()
                        break

                    # 3. Remove the processed bytes from buffer
                    del buffer[:consumed]

                    print(f"Processing: {request.command} {request.data}")

                    # 4. Route and Respond
                    try:
                        response = Router.route(request)

                        if response:
                            conn.sendall(response.to_bytes())
                    except Exception as e:
                        print(f"Handler Error: {e}")
                        # Send an error back to client so they aren't hanging
                        conn.sendall(f"-ERR {str(e)}\r\n".encode())

                    # request = RedisRequest.from_raw_data(raw_data)
                    # if not request.command:
                    #     continue

                    # print(f"request: {request}")
                    # print(f"request command: {request.command}")

                    # response = Router(command=request.command).route(request=request)
                    # print(f"response: {response}")

                    # # 4. Send Response
                    # if response:
                    #     conn.sendall(response.to_bytes())

        except (ConnectionResetError, BrokenPipeError):
            print(f"Client {addr} disconnected abruptly.")
        except Exception as e:
            print(f"Internal Error: {e}")
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
                client_thread.daemon = True # Ensure thread closes if main exits
                client_thread.start()
            except Exception as e:
                print(f"Error acception connection: {e}")
                break
