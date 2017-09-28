import asyncio
import collections
import logging
import os.path
import json

LOGGER = logging.getLogger(__name__)
QUEUES = {'default': asyncio.Queue(loop=asyncio.get_event_loop())}
QUEUES_PORTS = {'default': []}

MESSAGE_TYPES = collections.namedtuple(
    'MessageTypes', ('command', 'error', 'response')
)(*('command', 'error', 'response'))
COMMANDS = collections.namedtuple(
    'Commands', ('send', 'subscribe')
)(*('send', 'subscribe'))


def read_messages(files):
    for file in files:
        queue = file.split(".")[-2]
        QUEUES[queue] = asyncio.Queue(loop=asyncio.get_event_loop())
        QUEUES_PORTS[queue] = []
        for line in open(file, "r"):
            jline = json.loads(line)
            QUEUES[queue].put_nowait(jline['payload'])


@asyncio.coroutine
def send_to_subscribers(payload, queue):
    ports = QUEUES_PORTS[queue].copy()
    while ports:
        for port in QUEUES_PORTS[queue]:
            try:
                reader, writer = yield from asyncio.open_connection(
                    '127.0.0.1', port, loop=asyncio.get_event_loop()
                )
                writer.write(payload.encode('utf-8'))
                writer.write_eof()
                yield from writer.drain()
                writer.close()
                # reader.close()
            except Exception as ex:
                print("Could not connect: ", ex)
                break
            ports.remove(port)


@asyncio.coroutine
def handle_command(command, payload, queue, port):
    # LOGGER.debug('Handling command %s, payload %s', command, payload)
    if command not in COMMANDS:
        LOGGER.error('Got invalid command %s', command)
        raise ValueError('Invalid command. Should be one of %s' % (COMMANDS,))
    if command == COMMANDS.send:
        if queue not in QUEUES:
            QUEUES[queue] = asyncio.Queue(loop=asyncio.get_event_loop())
            QUEUES_PORTS[queue] = []
        yield from QUEUES[queue].put(payload)
        yield from send_to_subscribers(payload, queue)
        print(QUEUES_PORTS)
        msg = 'OK'
    elif command == COMMANDS.subscribe:
        if queue not in QUEUES:
            return {
                'type': MESSAGE_TYPES.error,
                'payload': "No such queue!"
            }
        msg = 'OK'
        QUEUES_PORTS[queue].append(port)
        print(QUEUES_PORTS)
    return {
        'type': MESSAGE_TYPES.response,
        'payload': msg
    }


@asyncio.coroutine
def dispatch_message(message):
    message_type = message.get('type')
    command = message.get('command')
    queue = message.get('queue')
    port = message.get('port')
    if message_type != MESSAGE_TYPES.command:
        LOGGER.error('Got invalid message type %s', message_type)
        raise ValueError('Invalid message type. Should be %s' % (MESSAGE_TYPES.command,))
    # LOGGER.debug('Dispatching command %s', command)
    response = yield from handle_command(command, message.get('payload'), queue, port)
    return response
