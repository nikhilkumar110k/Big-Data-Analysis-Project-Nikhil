import socket

host = '127.0.0.1'  
port = 5555  

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    client_socket.connect((host, port))
    print("Connected to server")

    while True:
        tweet = client_socket.recv(1024).decode('utf-8')
        if tweet:
            print(f"Received tweet: {tweet}")
        else:
            break  

except Exception as e:
    print(f"Error in client: {e}")
finally:
    client_socket.close() 
