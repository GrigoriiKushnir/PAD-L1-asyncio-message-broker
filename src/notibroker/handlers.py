import asyncio
import collections
import logging
import os.path
import json

LOGGER = logging.getLogger(__name__)
QUEUES = {'default': asyncio.Queue(loop=asyncio.get_event_loop())}

MESSAGE_TYPES = collections.namedtuple(
    'MessageTypes', ('command', 'error', 'response')
)(*('command', 'error', 'response'))
COMMANDS = collections.namedtuple(
    'Commands', ('send', 'read')
)(*('send', 'read'))


def read_messages(files):
    for file in files:
        queue = file.split(".")[-2]
        QUEUES[queue] = asyncio.Queue(loop=asyncio.get_event_loop())
        for line in open(file, "r"):
            jline = json.loads(line)
            QUEUES[queue].put_nowait(jline['payload'])

@asyncio.coroutine
def handle_command(command, payload, queue):
    # LOGGER.debug('Handling command %s, payload %s', command, payload)
    if command not in COMMANDS:
        LOGGER.error('Got invalid command %s', command)
        raise ValueError('Invalid command. Should be one of %s' % (COMMANDS,))
    if command == COMMANDS.send:
        if queue not in QUEUES:
            QUEUES[queue] = asyncio.Queue(loop=asyncio.get_event_loop())
        yield from QUEUES[queue].put(payload)
        msg = 'OK'
        print(QUEUES)
    elif command == COMMANDS.read:
        if queue not in QUEUES:
            return {
                'type': MESSAGE_TYPES.error,
                'payload': "No such queue!"
            }
        msg = yield from QUEUES[queue].get()
    return {
        'type': MESSAGE_TYPES.response,
        'payload': msg
    }


@asyncio.coroutine
def dispatch_message(message):
    message_type = message.get('type')
    command = message.get('command')
    if message.get('queue'):
        queue = message.get('queue')
    else:
        queue = 'default'
    if message_type != MESSAGE_TYPES.command:
        LOGGER.error('Got invalid message type %s', message_type)
        raise ValueError('Invalid message type. Should be %s' % (MESSAGE_TYPES.command,))
    # LOGGER.debug('Dispatching command %s', command)
    response = yield from handle_command(command, message.get('payload'), queue)
    return response
