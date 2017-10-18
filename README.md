#### Installation
To run this project you need Python>=3.4 (recommended 3.5).
[How to install it?](https://www.python.org/downloads/)

Pip (python package manager) should be installed with Python >= 3.4. However,
if you can't find it, check out [this page](https://pip.pypa.io/en/stable/installing/).

#### Dependencies

This project uses aiofiles library for async work with files.
This dependecy is stated in: `./requirements/local.txt`.
Just execute
`pip install -r requirements/local.txt`

#### Running project

This project has 3 components:
- Message broker;
- Client-publisher;
- Client-subscriber;

Message broker (`src/manage.py`) listens on `localhost:14141` for messages from clients.

Client-publisher (`src/sender.py`) sends each time interval a message, with randomly generated UUID,  **to** the broker.
Queues are created based on publisher's demand. If it writes to a nonexistent queue it is automatically created.
If queue's name ends with "_p" it will be persistent and written to a file with provided name and ".smq" extension.
Persistent queues are read in memory from files on broker start.
Non-persistent queues are considered as a "topic" and messages are routed directly to subscribers without storing.

Client-subscriber (`src/sender.py`) polls each second for a new message **from** the broker.
Subscriber can subscribe to queues using regex. It is used "\*" to match any character in desired queue name.
For example, for a given queues name standard say name.surname.nickname.alias you can use \*.\*.\*.alias to subscribe to all
alias queues. Or name.\*.\*.alias and so on. These can be shortened to \*alias or name\*alias.
Subscriber can provide a Last Will and Testament message and queue. Broker will detect abnormal disconnect of subscriber
and send the Last Will and Testament message to the given queue.
Subscriber sends every couple of seconds a keep_alive message to the broker.

See protocol-specs for more info about client-broker communication.

To run the broker just execute the `manage.py` script

`python3.5 src/manage.py`

To start publisher and respectively subscriber run

`python3.5 src/sender.py`

and

`python3.5 src/receiver.py`

Those scripts should be run from separated terminals.
And they'll provide output about performed operations.

*Note: Instead of `python3.5` you could try to use `python3.4` if you have it.*
