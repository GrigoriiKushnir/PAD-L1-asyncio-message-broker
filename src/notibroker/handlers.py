import asyncio
import collections
import logging
import json
from collections import defaultdict
import aiofiles
import re

LOGGER = logging.getLogger(__name__)
QUEUES = defaultdict(lambda: {'obj': asyncio.Queue(loop=asyncio.get_event_loop()), 'subs': []})

MESSAGE_TYPES = collections.namedtuple(
    'MessageTypes', ('command', 'error', 'response')
)(*('command', 'error', 'response'))
COMMANDS = collections.namedtuple(
    'Commands', ('send', 'subscribe', 'received')
)(*('send', 'subscribe', 'received'))


def read_messages(files):
    for file in files:
        queue = file.split(".")[-2]
        QUEUES[queue]
        for line in open(file, "r"):
            jline = json.loads(line)
            QUEUES[queue]['obj'].put_nowait(jline)


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
def delete_message(queue, message):
    file = queue + ".smq"
    f = yield from aiofiles.open(file, "r+")
    d = yield from f.readlines()
    yield from f.seek(0)
    try:
        for i in d:
            if i != json.dumps(message) + "\n":
                yield from f.write(i)
    finally:
        yield from f.truncate()
        yield from f.close()


def match_queues(queue):
    s = queue + "$"
    rex = s.replace("*", "(.*)")
    queues_list = []
    for i in QUEUES:
        try:
            queues_list.append(re.match(rex, i).group(0))
        except Exception:
            pass
    return queues_list


@asyncio.coroutine
def send_all(writer, reader, queue):
    while not QUEUES[queue]["obj"].empty():
        try:
            message = yield from QUEUES[queue]["obj"].get()
            writer.write(json.dumps(message).encode('utf-8'))
            reader.feed_data("feed".encode('utf-8'))
            data = yield from reader.read(1024)
            if data:
                yield from delete_message(queue, message)
                print(data)
            yield from writer.drain()
            yield from asyncio.sleep(0.5)
        except Exception as e:
            print("send_all error ", e)
            writer.close()
            return
    QUEUES[queue]['subs'].append((writer, reader))
    return "Subscribed to {}".format(queue)


@asyncio.coroutine
def send_to_subscribers(queue):
    if QUEUES[queue]['subs']:
        message = yield from QUEUES[queue]['obj'].get()
        for streams in QUEUES[queue]['subs']:
            try:
                writer = streams[0]
                reader = streams[1]
                # print(reader)
                reader.feed_data("feed".encode('utf-8'))
                writer.write(json.dumps(message).encode('utf-8'))
                data = (yield from reader.read(1024)).decode('utf8')
                yield from writer.drain()
                if not data or data == "feed":
                    print("No response, closing writer.")
                    writer.close()
                    QUEUES[queue]['subs'].remove(streams)
                    return
            except Exception as e:
                print("send_to_subscribers error: ", e)
                QUEUES[queue]['subs'].remove(streams)
                return "ERROR"
            if queue.endswith("_p"):
                yield from delete_message(queue, message)
    return "OK"


@asyncio.coroutine
def handle_command(message, writer, reader):
    command = message.get('command')
    queue = message.get('queue')
    # LOGGER.debug('Handling command %s, payload %s', command, payload)
    if command not in COMMANDS:
        LOGGER.error('Got invalid command %s', command)
        raise ValueError('Invalid command. Should be one of %s' % (COMMANDS,))

    if command == COMMANDS.send:
        persistent = queue.endswith("_p")
        yield from QUEUES[queue]['obj'].put(message)
        if persistent:
            yield from save_message(queue, message)
        msg = yield from send_to_subscribers(queue)

    elif command == COMMANDS.subscribe:
        queues_to_subscribe = match_queues(queue)
        print(queues_to_subscribe)
        if not queues_to_subscribe:
            return {
                'type': MESSAGE_TYPES.error,
                'payload': "No such queue!"
            }
        for q in queues_to_subscribe:
            if q.endswith("_p"):
                msg = yield from send_all(writer, reader, q)
            else:
                QUEUES[q]['subs'].append((writer, reader))
                msg = "Subscribed to {}".format(q)
    return {
        'type': MESSAGE_TYPES.response,
        'payload': msg
    }


@asyncio.coroutine
def dispatch_message(message, writer, reader):
    message_type = message.get('type')
    if message_type != MESSAGE_TYPES.command:
        LOGGER.error('Got invalid message type %s', message_type)
        raise ValueError('Invalid message type. Should be %s' % (MESSAGE_TYPES.command,))
    # LOGGER.debug('Dispatching command %s', command)
    response = yield from handle_command(message, writer, reader)
    return response
