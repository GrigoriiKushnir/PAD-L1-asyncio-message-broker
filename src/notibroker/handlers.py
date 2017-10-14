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
    'Commands', ('send', 'subscribe', 'received', 'disconnect')
)(*('send', 'subscribe', 'received', 'disconnect'))


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
def send_all(writer, reader, queue, sub_id):
    first = 0
    while not QUEUES[queue]["obj"].empty():
        try:
            message = yield from QUEUES[queue]["obj"].get()
            reader.feed_data("feed".encode('utf-8'))
            writer.write(json.dumps(message).encode('utf-8'))
            data = yield from reader.read(1024)
            print(data)
            if data.decode('utf-8') == "feed" and first == 1:
                # this is a graceful disconnect
                writer.close()
                return
            first = 1
            if data.decode('utf-8') != "feed":
                yield from delete_message(queue, message)
                # print("send_all", data)
            yield from writer.drain()
            yield from asyncio.sleep(0.1)
        except Exception as e:
            yield from QUEUES[queue]["obj"].put(message)
            print("send_all error ", e)
            # TODO: LWT
            print("LWT!")
            writer.close()
            return
    QUEUES[queue]['subs'].append((writer, reader, sub_id))
    return "Subscribed to {}".format(queue)


@asyncio.coroutine
def my_reader(reader):
    reader.feed_data("feed".encode('utf-8'))
    data = (yield from reader.read(1024)).decode('utf-8')
    while data.startswith('feed') or data == '':
        data = (yield from reader.read(1024)).decode('utf-8')
    return data


@asyncio.coroutine
def send_to_subscribers(queue, message):
    if QUEUES[queue]['subs']:
        for streams in QUEUES[queue]['subs']:
            writer = streams[0]
            sub_id = streams[2]
            try:
                writer.write(json.dumps(message).encode('utf-8'))
                yield from writer.drain()
            except Exception as e:
                if streams in QUEUES[queue]['subs']:
                    QUEUES[queue]['subs'].remove(streams)
                    writer.close()
                    print("send_to_subscribers error: ", e)
                    # TODO: LWT
                    print("LWT!", sub_id)
                    return
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
        if persistent:
            yield from save_message(queue, message)
        msg = yield from send_to_subscribers(queue, message)

    elif command == COMMANDS.subscribe:
        queues_to_subscribe = match_queues(queue)
        sub_id = message.get('sub_id')
        print("Subscribed", sub_id)
        # print(queues_to_subscribe)
        if not queues_to_subscribe:
            return {
                'type': MESSAGE_TYPES.error,
                'payload': "No such queue!"
            }
        for q in queues_to_subscribe:
            if q.endswith("_p"):
                msg = yield from send_all(writer, reader, q, sub_id)
            else:
                QUEUES[q]['subs'].append((writer, reader, sub_id))
                msg = "Subscribed to {}".format(q)
    elif command == COMMANDS.disconnect:
        sub_id = message.get('sub_id')
        print("Disconnected", message.get('sub_id'))
        for q in QUEUES:
            for sub in QUEUES[q]['subs']:
                if sub[2] == sub_id:
                    QUEUES[q]['subs'].remove(sub)

        msg = "Disconnect OK"
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
