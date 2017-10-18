## Description of the `notibroker` protocol

The protocol for the system `notibroker` is based on JSON.

`notibroker` understands queues as being persistent based on their name ending with "_p".

`type` is type of the message. It must be one of the following:
- `command` - perform a command;
- `response` - response message;
- `error` - error message;
- `received` - confirmation of message receivement;
- `lwt` - Last Will and Testament message;
- `check_alive` - connection alive check message.

`command` is type of action to be executed by the `notibroker` for the `command` message.

`notibroker` exposes the following commands:
- `send` - send a message to the queue;
- `subscribe` - subscribe to queue(s) topic(s) based on regex;
- `disconnect` - inform broker about planned disconnect of a subscriber;
- `keep_alive` - ask broker to keep the connection alive;

Examples of the structure for the `notibroker` command messages (dicts dumped to json):
```json
{
    "type": "command",
    "command": "send",
    "queue": "<queue>",
    "payload": "<message>"
}
```
```json
{
    "type": "command",
    "command": "subscribe",
    "queue": "<queue>",
    "sub_id": "<sub_id>",
    "lwt_queue": "<lwt_queue>",
    "lwt_message": "<lwt_message>"
}
```
```json
{
    "type": "command",
    "command": "disconnect",
    "sub_id": "<sub_id>"
}
```
```json
{
    "type": "command",
    "command": "keep_alive",
    "sub_id": "<sub_id>"
}
```

Examples of the messages' structure for other `notibroker` types:
```json
{
    "type": "response",
    "payload": "<payload>"
}
```
```json
{
    "type": "error",
    "payload": "<payload>"
}
```
```json
{
    "type": "lwt",
    "payload": "<lwt_message>"
}
```
```json
{
    "type": "received",
    "payload": "Message received",
}
```
```json
{
    "type": "check_alive",
    "payload": "Broker alive check"
}
```

`payload` is payload of sent/received message. For an `error` message, the `payload`
contains some info about the error.
