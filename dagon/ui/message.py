import enum
from typing import NamedTuple


class MessageType(enum.Enum):
    """
    Types of messages that a task can emit
    """
    Information = 'info'
    Warning = 'warn'
    Error = 'error'
    Fatal = 'fatal'
    Print = 'print'
    MetaPrint = 'meta-print'


class Message(NamedTuple):
    content: str
    type: MessageType
