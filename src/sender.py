import asyncio
import json
import uuid


@asyncio.coroutine
def send_message(message, loop, queue):
    reader, writer = yield from asyncio.open_connection(
        '127.0.0.1', 14141, loop=loop
    )
    payload = json.dumps({
        'type': 'command',
        'command': 'send',
        'queue': queue,
        'payload': message
    }).encode('utf-8')

    writer.write(payload)
    writer.write_eof()
    yield from writer.drain()
    response = yield from reader.read(2048)
    writer.close()
    return response


@asyncio.coroutine
def run_sender(loop):
    print("Queues ending with _p will be persistent.")
    queue = 'q1'
    # queue = input("Choose a queue: ")
    # while queue == '':
    #     queue = input("Choose a queue: ")
    while True:
        try:
            message = 'HI THERE %s' % (uuid.uuid4().hex,)
            print('Sending %s' % (message,))
            response = yield from send_message(message, loop, queue)
            print('Received %s' % (response.decode('utf-8'),))
            yield from asyncio.sleep(0.5)
        except Exception as e:
            print("ERROR:", e)
            break


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_sender(loop))


if __name__ == '__main__':
    main()
