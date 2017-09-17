import asyncio
import json
import logging
import aiofiles

from .handlers import dispatch_message  # , read_messages

LOGGER = logging.getLogger(__name__)


@asyncio.coroutine
def save_message(message):
    if message.get('queue'):
        file = message.get('queue')
    else:
        file = 'default'
    f = yield from aiofiles.open(file, mode='a+')
    try:
        yield from f.write(json.dumps(message) + "\n")
    finally:
        yield from f.close()
        # print("Saved:", str(message))


@asyncio.coroutine
def delete_message(message):
    if message.get('queue'):
        file = message.get('queue')
    else:
        file = 'default'
    f = yield from aiofiles.open(file, mode='r+')
    try:
        lines = yield from f.readlines()
        lines = lines[:-1]
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
        if message['command'] == "send":
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
        if message['command'] == "read":
            try:
                yield from delete_message(message)
            except Exception as ex:
                LOGGER.debug('Wrong queue name from %s', address)
    except ValueError as e:
        LOGGER.exception('Cannot process the message. %s')
        send_error(writer, str(e))

    writer.close()


def run_server(hostname='localhost', port=14141, loop=None):
    # read_messages('messages')
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
