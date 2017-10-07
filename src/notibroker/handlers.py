import asyncio
import collections
import logging
import json
from collections import defaultdict
import aiofiles

LOGGER = logging.getLogger(__name__)
QUEUES = defaultdict(lambda: {'obj': asyncio.Queue(loop=asyncio.get_event_loop()), 'subs': []})
QUEUES['default']

MESSAGE_TYPES = collections.namedtuple(
    'MessageTypes', ('command', 'error', 'response')
)(*('command', 'error', 'response'))
COMMANDS = collections.namedtuple(
    'Commands', ('send', 'subscribe', 'read_all')
)(*('send', 'subscribe', 'read_all'))


def read_messages(files):
    for file in files:
        queue = file.split(".")[-2]
        for line in open(file, "r"):
            jline = json.loads(line)
            QUEUES[queue]['obj'].put_nowait(jline['payload'])


@asyncio.coroutine
def save_message(queue, message):
    file = queue + ".smq"
    f = yield from aiofiles.open(file, mode='a+')
    try:
        yield from f.write(json.dumps(message) + "\n")
    finally:
        yield from f.close()
        # print("Saved:", str(message))


@asyncio.coroutine
def delete_message(queue):
    file = queue + ".smq"
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
def send_all(writer, queue):
    while not QUEUES[queue]["obj"].empty():
        try:
            message = yield from QUEUES[queue]["obj"].get()
            writer.write(message.encode('utf-8'))
            yield from delete_message(queue)
            yield from writer.drain()
            yield from asyncio.sleep(0.1)
        except Exception as e:
            print("1 Can't send to subscriber, removing: ", e)
            if writer in QUEUES[queue]['subs']:
                QUEUES[queue]['subs'].remove(writer)
                writer.close()
            break


@asyncio.coroutine
def send_to_subscribers(payload, queue):
    for writer in QUEUES[queue]['subs']:
        try:
            writer.write(payload.encode('utf-8'))
            yield from writer.drain()
        except Exception as e:
            print("2 Can't send to subscriber, removing: ", e)
            if writer in QUEUES[queue]['subs']:
                QUEUES[queue]['subs'].remove(writer)
                writer.close()


@asyncio.coroutine
def handle_command(message, writer):
    command = message.get('command')
    read_all = message.get('read_all')
    queue = message.get('queue')
    payload = message.get('payload')
    persistent = queue.endswith("_p")
    # LOGGER.debug('Handling command %s, payload %s', command, payload)
    if command not in COMMANDS:
        LOGGER.error('Got invalid command %s', command)
        raise ValueError('Invalid command. Should be one of %s' % (COMMANDS,))

    if command == COMMANDS.send:
        yield from QUEUES[queue]['obj'].put(payload)
        if persistent:
            yield from save_message(queue, message)
        yield from send_to_subscribers(payload, queue)
        msg = 'OK'

    elif command == COMMANDS.subscribe:
        if queue not in QUEUES:
            return {
                'type': MESSAGE_TYPES.error,
                'payload': "No such queue!"
            }
        if read_all and persistent:
            yield from send_all(writer, queue)
        QUEUES[queue]['subs'].append(writer)
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
