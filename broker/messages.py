from dataclasses import dataclass, asdict


@dataclass
class Message:
    @staticmethod
    def from_dict(dict_data: dict) -> "Message":
        msg_type = dict_data.pop("type")

        match msg_type:
            case "NewConnection":
                return NewConnection()
            case "AuthenticatedMessage":
                return AuthenticatedMessage(**dict_data)
            case "BeginProcessingRequest":
                return BeginProcessingRequest(**dict_data)
            case "ProcessingRequestPermission":
                return ProcessingRequestPermission(**dict_data)
            case "ContinueProcessingRequest":
                return ContinueProcessingRequest(**dict_data)
            case "UnknownMessage":
                return UnknownMessage()
            case _:
                raise ValueError(f"Unknown message type: {dict_data['type']}")

    @staticmethod
    def to_json(message: "Message") -> dict:
        data_dict = asdict(message)
        data_dict["type"] = message.__class__.__name__

        return data_dict


@dataclass
class NewConnection(Message):
    pass


@dataclass
class UnknownMessage(Message):
    pass


@dataclass
class AuthenticatedMessage(Message):
    client_id: str


@dataclass
class BeginProcessingRequest(AuthenticatedMessage):
    event_name: str


@dataclass
class ProcessingRequestPermission(Message):
    allowed: bool
    lease: float


@dataclass
class ContinueProcessingRequest(AuthenticatedMessage):
    event_name: str


