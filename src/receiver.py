#!/usr/bin/env python3
import json
import socket
import random


def main():
    # queue = input("Choose a queue: ")
    queue = "q1_p"
    port = random.randint(1024, 65535)
    print(port)
    searching = 1

    while searching:
        try:
            serversocket = socket.socket()
            serversocket.bind(('', port))
            serversocket.listen(1)
            searching = 0
        except Exception:
            print("Could not listen, searching another port.")

    sock = socket.socket()
    message = json.dumps({
        'type': 'command',
        'command': 'subscribe',
        'queue': queue,
        'port': port
    }).encode('utf-8')
    sock.connect(('localhost', 14141))
    sock.send(message)
    sock.close()

    while True:
        client, addr = serversocket.accept()
        while True:
            data = client.recv(1024)
            if not data:
                break
            print(data.decode('utf8'))

if __name__ == '__main__':
    main()
