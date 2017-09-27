import asyncio
import json
import logging
import aiofiles
import os
from threading import Thread

from .handlers import dispatch_message, read_messages

LOGGER = logging.getLogger(__name__)


@asyncio.coroutine
def save_message(message):
    if message.get('queue'):
        file = message.get('queue') + ".smq"
    else:
        file = 'default.smq'
    f = yield from aiofiles.open(file, mode='a+')
    try:
        yield from f.write(json.dumps(message) + "\n")
    finally:
        yield from f.close()
        # print("Saved:", str(message))


@asyncio.coroutine
def delete_message(message):
    if message.get('queue'):
        file = message.get('queue') + ".smq"
    else:
        file = 'default' + ".smq"
    f = yield from aiofiles.open(file, mode='r+')
    try:
        lines = yield from f.readlines()
        lines = lines[1:]
    finally:
        yield from f.close()
    f = yield from aiofiles.open(file, mode='w')
    try:
        yield from f.writelines(lines)
    finally:
        yield from f.close()
        # print("Sent:", str(message)


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
def handle_message(reader, writer):
    data = yield from reader.read()
    address = writer.get_extra_info('peername')
    # LOGGER.debug('Recevied message from %s', address)
    try:
        message = json.loads(data.decode('utf-8'))
        if message['command'] == "send" and message['queue'].endswith("_p"):
            yield from save_message(message)
            # LOGGER.debug('Saved message: %s', str(message))
    except ValueError as e:
        LOGGER.exception('Invalid message received')
        send_error(writer, str(e))
        return
    try:
        response = yield from dispatch_message(message)
        payload = json.dumps(response).encode('utf-8')
        writer.write(payload)
        yield from writer.drain()
        writer.write_eof()
        if message['command'] == "subscribe":
            # TODO: can listen, have to implement broker now ( send message when received? )
            if message['queue'].endswith("_p"):
                try:
                    pass
                    # yield from delete_message(message)
                except Exception:
                    LOGGER.debug('Wrong queue name from %s', address)
    except ValueError as e:
        LOGGER.exception('Cannot process the message. %s')
        send_error(writer, str(e))

    writer.close()


# @asyncio.coroutine
# def respond(reader, writer):
#     data = yield from reader.read()
#     message = json.loads(data.decode('utf-8'))
#     port = message['port']
#     print(message)
#     while True:
#         try:
#             reader1, writer1 = yield from asyncio.open_connection(
#                 '127.0.0.1', port, loop=asyncio.get_event_loop()
#             )
#             payload = json.dumps({
#                 'type': 'command',
#                 'command': 'send',
#                 'payload': "himan"
#             }).encode('utf-8')
#
#             writer1.write(payload)
#             writer1.write_eof()
#             yield from writer.drain()
#             writer1.close()
#             yield from asyncio.sleep(1)
#         except Exception as ex:
#             print("Could not connect: ", ex)
#             break


def run_server(hostname='localhost', port=14141, loop=None):
    files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith(".smq"):
            files.append(file)
    read_messages(files)
    if loop is None:
        loop = asyncio.get_event_loop()
    coro = asyncio.start_server(handle_command, hostname, port, loop=loop)
    server = loop.run_until_complete(coro)
    LOGGER.info('Serving on %s', server.sockets[0].getsockname())
    LOGGER.info('Press Ctrl + C to stop the application')
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
