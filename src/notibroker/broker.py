import asyncio
import json
import logging
import os

from .handlers import dispatch_message, read_messages

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
def handle_message(reader, writer):
    data = yield from reader.read()
    # address = writer.get_extra_info('peername')
    # LOGGER.debug('Recevied message from %s', address)
    try:
        message = json.loads(data.decode('utf-8'))
        response = yield from dispatch_message(message, writer)
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
    coro = asyncio.start_server(handle_message, hostname, port, loop=loop)
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
