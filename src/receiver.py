import asyncio
import json


@asyncio.coroutine
def get_message(loop, queue, read_all):
    reader, writer = yield from asyncio.open_connection(
        '127.0.0.1', 14141, loop=loop
    )
    writer.write(json.dumps({
        'type': 'command',
        'command': 'subscribe',
        'read_all': read_all,
        'queue': queue
    }).encode('utf-8'))
    writer.write_eof()
    while True:
        response = (yield from reader.read(1024)).decode('utf8')
        if response != '':
            print(response)


def main():
    # queue = input("Choose a queue: ")
    # read_all = input("Read persistent messages? 1/0: ")
    queue = "q1_p"
    read_all = 0
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_message(loop, queue, read_all))


if __name__ == '__main__':
    main()
