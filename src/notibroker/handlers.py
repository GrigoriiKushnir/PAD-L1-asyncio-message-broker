import asyncio
import collections
import logging
import json

LOGGER = logging.getLogger(__name__)
QUEUES = {'default': asyncio.Queue(loop=asyncio.get_event_loop())}
QUEUES_WRITERS = {'default': []}

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
        QUEUES_WRITERS[queue] = []
        for line in open(file, "r"):
            jline = json.loads(line)
            QUEUES[queue].put_nowait(jline['payload'])


@asyncio.coroutine
def send_to_subscribers(message, queue):
    for writer in QUEUES_WRITERS[queue]:
        data = json.dumps(message)
        writer.write(data.encode('utf-8'))


@asyncio.coroutine
def handle_command(message, writer):
    command = message.get('command')
    queue = message.get('queue')
    payload = message.get('payload')
    # LOGGER.debug('Handling command %s, payload %s', command, payload)
    if command not in COMMANDS:
        LOGGER.error('Got invalid command %s', command)
        raise ValueError('Invalid command. Should be one of %s' % (COMMANDS,))

    if command == COMMANDS.send:
        if queue not in QUEUES:
            QUEUES[queue] = asyncio.Queue(loop=asyncio.get_event_loop())
            QUEUES_WRITERS[queue] = []
        yield from QUEUES[queue].put(payload)
        yield from send_to_subscribers(message, queue)
        msg = 'OK'

    elif command == COMMANDS.subscribe:
        if queue not in QUEUES:
            return {
                'type': MESSAGE_TYPES.error,
                'payload': "No such queue!"
            }
        QUEUES_WRITERS[queue].append(writer)
        msg = 'OK'

    return {
        'type': MESSAGE_TYPES.response,
        'payload': msg
    }


@asyncio.coroutine
def dispatch_message(message, writer):
    message_type = message.get('type')
    if message_type != MESSAGE_TYPES.command:
        LOGGER.error('Got invalid message type %s', message_type)
        raise ValueError('Invalid message type. Should be %s' % (MESSAGE_TYPES.command,))
    # LOGGER.debug('Dispatching command %s', command)
    response = yield from handle_command(message, writer)
    return response
