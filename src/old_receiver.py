import asyncio
import json

@asyncio.coroutine
def get_message(loop):
    reader, writer = yield from asyncio.open_connection(
        '127.0.0.1', 14141, loop=loop
    )
    writer.write(json.dumps({
        'type': 'command',
        'command': 'read'
    }).encode('utf-8'))
    writer.write_eof()
    yield from writer.drain()
    response = yield from reader.read()
    writer.close()
    return response


@asyncio.coroutine
def run_receiver(loop):
    while True:
        try:
            response = yield from get_message(loop)
            print('Received %s', response)
            yield from asyncio.sleep(1)
        except KeyboardInterrupt:
            break


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_receiver(loop))


if __name__ == '__main__':
    main()