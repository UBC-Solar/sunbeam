from enum import StrEnum

import zmq
from broker.messages import Message, NewConnection, AuthenticatedMessage, UnknownMessage
from coolname import generate_slug
from dataclasses import dataclass
from typing import Optional

from config.context import Context


class ConnectionState(StrEnum):
    CONNECTED = "connected"
    WAITING = "waiting"
    WRITING = "writing"


@dataclass
class Connection:
    connection_state: ConnectionState
    event_name: Optional[str]
    connection_id: str


class Server:
    def __init__(self) -> None:
        self._context = zmq.Context()
        self._address = Context().sunbeam_broker.build_url()

        print(f"Binding on {self._address}...", flush=True)
        self._socket = self.bind(self._address)
        self._connections = {}

        self.run()

    def bind(self, address: str) -> zmq.Socket:
        socket  = self._context.socket(zmq.REP)
        socket.bind(address)

        return socket

    def __del__(self):
        print("Exiting...", flush=True)
        self._socket.close()

    def run(self):
        print("Starting server...", flush=True)
        while True:
            raw_msg = self._socket.recv_json()
            print("Receiving message...", flush=True)
            msg = Message.from_dict(raw_msg)
            print(msg)

            if isinstance(msg, NewConnection):
                new_connection_id = generate_slug(2)
                new_connection = Connection(
                    connection_state=ConnectionState.CONNECTED,
                    connection_id=new_connection_id,
                    event_name=None)
                self._connections[new_connection_id] = new_connection

                print("New connection: ", new_connection_id, flush=True)

                response = AuthenticatedMessage(client_id=new_connection_id)
                self._socket.send_json(Message.to_json(response))
                print("Responded.", flush=True)

            else:
                print("Replying with unknown message...", flush=True)
                response = UnknownMessage()
                self._socket.send_json(Message.to_json(response))