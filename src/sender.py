#!/usr/bin/env python3
import asyncio
import json
import uuid


@asyncio.coroutine
def send_message(message, loop):
    reader, writer = yield from asyncio.open_connection(
        '127.0.0.1', 14141, loop=loop
    )
    payload = json.dumps({
        'type': 'command',
        'command': 'send',
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
    while True:
        try:
            message = 'Just sending a random UUID %s' % (uuid.uuid4().hex,)
            print('Sending %s' % (message,))
            response = yield from send_message(message, loop)
            print('Received %s', response)
            yield from asyncio.sleep(1)
        except KeyboardInterrupt:
            break


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_sender(loop))


if __name__ == '__main__':
    main()
