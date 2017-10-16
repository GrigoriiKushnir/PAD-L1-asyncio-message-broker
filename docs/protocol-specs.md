## Description of the `notibroker` protocol

The protocol for the system `notibroker` is based on JSON.
`notibroker` exposes the following commands:

- `send` - send a message to the queue;
- `subscribe` - subscribe to queue(s) topic(s) based on regex;
- `disconnect` - inform broker about planned disconnect of a subscriber;
- `keep_alive` - inform broker about alive connection;

Example of the structure for a `notibroker` message (dicts dumped to json):

{
    "type": "command",
    "command": "send",
    "queue": "<queue>",
    "payload": "<message>"
}

{
    "type": "command",
    "command": "subscribe",
    "queue": "<queue>",
    "sub_id": "<sub_id>",
    "lwt_queue": "<lwt_queue>",
    "lwt_message": "<lwt_message>"
}

{
    "type": "lwt",
    "payload": "<lwt_message>"
}

{
    "type": "response",
    "payload": "<payload>"
}

{
    "type": "error",
    "payload": "<payload>"
}


`type` is type of the message. It must be one of the following:
- `command`
- `response`
- `error`
- `received`
- `lwt`
- `check_alive`

`payload` is payload of sent/received message. For an `error` message, the `payload`
contains some info about the error.

`command` is type of action to be executed by the `notibroker` for the `command` message.
