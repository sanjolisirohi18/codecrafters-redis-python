from .tcp_server import TcpServer

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    # server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # server_socket.accept() # wait for client

    # Instantiate and start the server
    tcp_server = TcpServer(
        host="localhost",
        port=7639
    )

    tcp_server.start()


if __name__ == "__main__":
    main()
