import asyncio
import json
import logging
import os
import time

from .handlers import dispatch_message, read_messages, send_to_subscribers, QUEUES, ALIVE, LWT, MESSAGE_TYPES

LOGGER = logging.getLogger(__name__)


@asyncio.coroutine
def send_error(writer, reason):
    message = {
        'type': 'error',
        'payload': reason
    }
    payload = json.dumps(message).encode('utf-8')
    writer.write(payload)
    yield from writer.drain()


@asyncio.coroutine
def check_alive():
    while True:
        yield from asyncio.sleep(5)
        for q in QUEUES:
            for sub in QUEUES[q]['subs']:
                sub_id = sub[2]
                sub_writer = sub[0]
                lwt_message = {
                    'type': MESSAGE_TYPES.lwt,
                    'payload': LWT[sub_id][1]
                }
                lwt_queue = LWT[sub_id][0]
                try:
                    sub_writer.write(json.dumps({
                        'type': MESSAGE_TYPES.check_alive,
                        'payload': "Broker alive check"
                    }).encode('utf-8'))
                    yield from sub_writer.drain()
                except ConnectionError:
                    del ALIVE[sub_id]
                    QUEUES[q]['subs'].remove(sub)
                    del LWT[sub_id]
                    yield from send_to_subscribers(lwt_queue, lwt_message)
                    LOGGER.debug("Lwt sent - transport: %s", sub_id)
                if sub_id in ALIVE:
                    if time.time() - ALIVE[sub_id] > 5:
                        # print("diff: ", time.time() - ALIVE[sub_id])
                        # print("sub_id", sub_id)
                        del ALIVE[sub_id]
                        QUEUES[q]['subs'].remove(sub)
                        del LWT[sub_id]
                        yield from send_to_subscribers(lwt_queue, lwt_message)
                        LOGGER.debug("Lwt sent - app: %s", sub_id)


@asyncio.coroutine
def handle_message(reader, writer):
    data = yield from reader.read(1024)
    # LOGGER.debug('Recevied message from %s', address)
    try:
        message = json.loads(data.decode('utf-8'))
        response = yield from dispatch_message(message, writer, reader)
        payload = json.dumps(response).encode('utf-8')
        writer.write(payload)
    except Exception as e:
        LOGGER.exception('Cannot process the message. %s')
        send_error(writer, str(e))


def run_server(hostname='localhost', port=14141, loop=None):
    files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith(".smq"):
            files.append(file)
    read_messages(files)
    if loop is None:
        loop = asyncio.get_event_loop()
    task = asyncio.Task(check_alive())
    coro = asyncio.start_server(handle_message, hostname, port, loop=loop)
    server = loop.run_until_complete(coro)
    LOGGER.info('Serving on %s', server.sockets[0].getsockname())
    LOGGER.info('Press Ctrl + C to stop the application')
    try:
        loop.run_forever()
    except Exception as e:
        print("serv", e)
        pass
    server.close()

    loop.run_until_complete(server.wait_closed())
    loop.run_until_complete(task)
    loop.close()
