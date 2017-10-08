import asyncio
import json
import uuid


@asyncio.coroutine
def get_message(loop, queue):
    reader, writer = yield from asyncio.open_connection(
        '127.0.0.1', 14141, loop=loop
    )
    writer.write(json.dumps({
        'type': 'command',
        'command': 'subscribe',
        'queue': queue,
    }).encode('utf-8'))
    # writer.write_eof()
    while True:
        response = (yield from reader.read(1024)).decode('utf8')
        if response != '':
            print(response)
            writer.write(json.dumps({
                'type': 'command',
                'command': 'received',
            }).encode('utf-8'))


def main():
    # queue = input("Choose a queue: ")
    # while queue == '':
    #     queue = input("Choose a queue: ")
    queue = "*"
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_message(loop, queue))


if __name__ == '__main__':
    main()
