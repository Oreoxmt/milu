import asyncio
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from queue import Queue
from typing import Dict

from prisma import Prisma
from prisma.models import Message as PrismaMessage


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
        prisma_message: PrismaMessage,
    ):
        self._prisma_message = prisma_message
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
            f"Message(id={self.id}, role={self.role.value}, content={self.content}, "
            f"status={self.status}, external_id={self.external_id})"
        )

    def append_token(self, token: str):
        """
        Append a token to the message.
        :param token: the token to append.
        """
        if self.content is None:
            self.content = ""
        self.content += token
        self._tokens.put(token)

    def finish(self):
        """
        Finish the message.
        """
        self._tokens.put(None)

    @property
    def id(self) -> str:
        return self._prisma_message.id

    @property
    def role(self) -> MessageRole:
        return MessageRole(self._prisma_message.role)

    @property
    def content(self) -> str | None:
        return self._prisma_message.content

    @content.setter
    def content(self, value: str | None):
        task = asyncio.create_task(
            self._prisma_message.prisma().update(
                data={"content": value}, where={"id": self.id}
            )
        )

    @property
    def parent_id(self) -> str | None:
        return self._prisma_message.parent_id

    @parent_id.setter
    def parent_id(self, value: str | None):
        if value is None:
            self._prisma_message.prisma().update(
                data={
                    "parent": {
                        "disconnect": True,
                    },
                },
                where={"id": self.id},
            )
        else:
            self._prisma_message.prisma().update(
                data={
                    "parent": {
                        "connect": {
                            "id": value,
                        },
                    }
                },
                where={"id": self.id},
            )

    @property
    def status(self) -> MessageStatus | None:
        return MessageStatus(self._prisma_message.status)

    @status.setter
    def status(self, value: MessageStatus | None):
        self._prisma_message.prisma().update(
            data={"status": None if value is None else value.value}, where={"id": self.id}
        )

    @property
    def external_id(self) -> str | None:
        return self._prisma_message.external_id

    @external_id.setter
    def external_id(self, value: str):
        self._prisma_message.prisma().update(
            data={"external_id": value}, where={"id": self.id}
        )


class Core:
    def __init__(self):
        self._messages: Dict[str, Message] = {}
        self._client = Prisma(auto_register=True)

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
            await self._client.message.create(
                {
                    "id": str(uuid.uuid4()),
                    "role": opt.role.value,
                    "content": opt.content,
                    "parent_id": parent_id,
                    "status": None,
                    "external_id": opt.external_id,
                }
            )
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
