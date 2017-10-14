import asyncio
import json
import uuid
import time

@asyncio.coroutine
def get_message(loop, queue):
    sub_id = uuid.uuid4().hex
    print(sub_id)
    reader, writer = yield from asyncio.open_connection(
        '127.0.0.1', 14141, loop=loop
    )
    writer.write(json.dumps({
        'type': 'command',
        'command': 'subscribe',
        'queue': queue,
        'sub_id': sub_id
    }).encode('utf-8'))
    # writer.write_eof()
    counter = 0
    while True:
        response = (yield from reader.read(1024)).decode('utf8')
        if response != '':
            print(response)
            counter += 1
            writer.write(json.dumps({
                'type': 'command',
                'command': 'received',
            }).encode('utf-8'))
            # if counter == 3:
            #     reader, writer = yield from asyncio.open_connection(
            #         '127.0.0.1', 14141, loop=loop
            #     )
            #     writer.write(json.dumps({
            #         'type': 'command',
            #         'command': 'disconnect',
            #         'sub_id': sub_id
            #     }).encode('utf-8'))


def main():
    # queue = input("Choose a queue: ")
    # while queue == '':
    #     queue = input("Choose a queue: ")
    queue = "*"
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_message(loop, queue))


if __name__ == '__main__':
    main()
