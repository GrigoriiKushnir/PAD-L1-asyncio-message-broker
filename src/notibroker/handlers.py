import asyncio
import collections
import logging

LOGGER = logging.getLogger(__name__)
_MESSAGE_QUEUE = asyncio.Queue(loop=asyncio.get_event_loop())

MESSAGE_TYPES = collections.namedtuple(
    'MessageTypes', ('command', 'error', 'response')
)(*('command', 'error', 'response'))
COMMANDS = collections.namedtuple(
    'Commands', ('send', 'read')
)(*('send', 'read'))


@asyncio.coroutine
def handle_command(command, payload):
    # LOGGER.debug('Handling command %s, payload %s', command, payload)
    if command not in COMMANDS:
        LOGGER.error('Got invalid command %s', command)
        raise ValueError('Invalid command. Should be one of %s' % (COMMANDS,))
    if command == COMMANDS.send:
        yield from _MESSAGE_QUEUE.put(payload)
        msg = 'OK'
    elif command == COMMANDS.read:
        msg = yield from _MESSAGE_QUEUE.get()
    return {
        'type': MESSAGE_TYPES.response,
        'payload': msg
    }


@asyncio.coroutine
def dispatch_message(message):
    message_type = message.get('type')
    command = message.get('command')
    if message_type != MESSAGE_TYPES.command:
        LOGGER.error('Got invalid message type %s', message_type)
        raise ValueError('Invalid message type. Should be %s' % (MESSAGE_TYPES.command,))
    # LOGGER.debug('Dispatching command %s', command)
    response = yield from handle_command(command, message.get('payload'))
    return response
