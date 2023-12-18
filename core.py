import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from queue import Queue
from typing import Dict

from prisma import Prisma


class MessageStatus(Enum):
    PENDING = "pending"
    GENERATING = "generating"
    FINISHED = "finished"
    ERROR = "error"


class MessageRole(Enum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


@dataclass
class AppendOption:
    role: MessageRole
    content: str | None = None
    external_id: str | None = None


class Message:
    def __init__(
        self,
        id: str,
        role: MessageRole,
        content: str | None,
        parent_id: str | None,
        status: MessageStatus | None,
        external_id: str | None,
    ):
        """
        :param id: the UUID of the message, which is a unique identifier in the database.
        :param role: the role of the message. It can be "user", "assistant", "system".
        :param content: the content of the message.
        :param parent_id: the parent message of the message. If it is None, this message is a root message (system prompt).
        :param status: the status of the message.
        :param external_id: the source ID of the message, such as "telegram:1234567890".
        """
        self._id = id
        self._role = role
        self._content = content
        self._parent_id = parent_id
        self._status = status
        self._external_id = external_id
        # Use queue to store the tokens of the message.
        self._tokens: Queue[str | None] = Queue()

    def __iter__(self):
        return self

    def __next__(self) -> str:
        next_token = self._tokens.get()
        if next_token is None:
            raise StopIteration
        return next_token

    def __repr__(self):
        return (
            f"Message(id={self._id}, role={self._role.value}, content={self._content}, "
            f"status={self._status}, external_id={self._external_id})"
        )

    def append_token(self, token: str):
        """
        Append a token to the message.
        :param token: the token to append.
        """
        if self._content is None:
            self._content = ""
        self._content += token
        self._tokens.put(token)

    def finish(self):
        """
        Finish the message.
        """
        self._tokens.put(None)

    @property
    def id(self) -> str:
        return self._id

    @property
    def role(self) -> MessageRole:
        return self._role

    @property
    def content(self) -> str | None:
        return self._content

    @property
    def parent_id(self) -> str | None:
        return self._parent_id

    @property
    def status(self) -> MessageStatus | None:
        return self._status

    @status.setter
    def status(self, value: MessageStatus | None):
        self._status = value

    @property
    def external_id(self) -> str | None:
        return self._external_id

    @external_id.setter
    def external_id(self, value: str):
        self._external_id = value


class Core:
    def __init__(self):
        self._messages: Dict[str, Message] = {}
        self._client = Prisma()

    async def append(self, parent: Message | str | None, opt: AppendOption) -> Message:
        """
        Append a message to the parent message.
        :param parent: the parent message object or the source ID of the message.
        :param opt: the options of the message.
        """
        if opt.role == MessageRole.SYSTEM:
            if parent is not None:
                raise Exception("The parent of a system message must be None.")
            if opt.content is None:
                raise Exception("The content of a system message cannot be None.")
        elif opt.role == MessageRole.USER:
            if parent is None:
                raise Exception("The parent of a user message cannot be None.")
            if opt.content is None:
                raise Exception("The content of a user message cannot be None.")
        elif opt.role == MessageRole.ASSISTANT:
            if parent is None:
                raise Exception("The parent of an assistant message cannot be None.")
            if opt.content is not None:
                raise Exception("The content of an assistant message must be None.")
        else:
            raise Exception("Invalid message role.")
        parent_id = parent.id if isinstance(parent, Message) else parent
        new_message = Message(
            id=str(uuid.uuid4()),
            role=opt.role,
            content=opt.content,
            parent_id=parent_id,
            status=None,
            external_id=opt.external_id,
        )
        if opt.role == MessageRole.ASSISTANT:
            new_message.status = MessageStatus.PENDING
            threading.Thread(target=fake_api, args=(new_message,)).start()
        self._messages[new_message.id] = new_message
        return new_message


def fake_api(message: Message):
    message.status = MessageStatus.GENERATING
    for i in range(10):
        message.append_token(str(i))
        time.sleep(1)
    message.finish()
    message.status = MessageStatus.FINISHED
