import zmq
from broker.messages import Message, NewConnection, AuthenticatedMessage
from config.context import Context


class Client:
    def __init__(self):
        self._context = zmq.Context()
        self._address = Context().sunbeam_broker.build_url()

        print(f"Connecting on {self._address}", flush=True)
        self._socket = self.connect_socket(self._address)
        self.id = None

    def connect_socket(self, address: str) -> zmq.Socket:
        socket = self._context.socket(zmq.REQ)
        socket.connect(address)

        return socket

    def connect(self):
        msg = NewConnection()
        print("Sending message...", flush=True)

        self._socket.send_json(Message.to_json(msg))
        raw_response = self._socket.recv_json()
        print("Got response...", flush=True)
        response = Message.from_dict(raw_response)

        if isinstance(response, AuthenticatedMessage):
            self.id = response.client_id
            print("Client ID: " + str(self.id), flush=True)
        else:
            raise RuntimeError(f"Got wrong response type: {type(response)}")
