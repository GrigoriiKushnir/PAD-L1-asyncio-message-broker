import asyncio
import json
import uuid


@asyncio.coroutine
def keep_alive(loop, sub_id):
    while True:
        reader, writer = yield from asyncio.open_connection(
            '127.0.0.1', 14141, loop=loop
        )
        writer.write(json.dumps({
            'type': 'command',
            'command': 'keep_alive',
            'sub_id': sub_id,
        }).encode('utf-8'))
        yield from asyncio.sleep(3)
        print("keep_alive sent")


@asyncio.coroutine
def get_message(loop, queue, lwt_queue, sub_id):
    lwt_message = "LWT from {}".format(sub_id)
    reader, writer = yield from asyncio.open_connection(
        '127.0.0.1', 14141, loop=loop
    )
    writer.write(json.dumps({
        'type': 'command',
        'command': 'subscribe',
        'queue': queue,
        'sub_id': sub_id,
        'lwt_queue': lwt_queue,
        'lwt_message': lwt_message,
    }).encode('utf-8'))
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
    sub_id = uuid.uuid4().hex
    print(sub_id)
    # queue = input("Choose a queue: ")
    # while queue == '':
    #     queue = input("Choose a queue: ")
    # queue = input("Choose a LWT queue: ")
    # while queue == '':
    #     queue = input("Choose a LWT queue: ")
    queue = "q1"
    lwt_queue = 'q1'
    loop = asyncio.get_event_loop()
    task = asyncio.Task(keep_alive(loop, sub_id))
    loop.run_until_complete(get_message(loop, queue, lwt_queue, sub_id))
    loop.run_until_complete(task)


if __name__ == '__main__':
    main()
