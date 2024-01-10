import asyncio
import uuid
from asyncio import Queue
from dataclasses import dataclass
from types import TracebackType
from typing import Dict, Type

from milu.db.prisma import Prisma
from milu.db.prisma.models import Message as PrismaMessage
from milu.db.prisma.types import (
    MessageUpdateInput,
    MessageUpdateOneWithoutRelationsInput,
    MessageWhereUniqueInput,
)

PENDING = "pending"
GENERATING = "generating"
FINISHED = "finished"
ERROR = "error"
USER = "user"
ASSISTANT = "assistant"
SYSTEM = "system"


@dataclass
class AppendOption:
    role: str
    content: str | None = None
    external_id: str | None = None


class Message:
    def __init__(
        self,
        prisma_message: PrismaMessage,
    ):
        self._prisma_message: PrismaMessage = prisma_message
        self._data: MessageUpdateInput | None = None
        self._tokens: Queue[str | None] = Queue()

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        next_token = await self._tokens.get()
        if next_token is None:
            raise StopAsyncIteration
        return next_token

    async def __aenter__(self):
        if self._data is not None:
            raise Exception("The message is already in the context.")
        self._data = {}
        return self

    async def __aexit__(
        self,
        exc_type: Type[Exception] | None,
        exc_val: Type[Exception] | None,
        exc_tb: TracebackType | None,
    ):
        if self._data is None:
            raise Exception("The message is not in the context.")
        if exc_type is not None:
            print(exc_type)
            print(exc_val)
            print(exc_tb)
        await self._update_async(self._data, None)
        self._data = None

    def __repr__(self):
        return (
            f"Message(id={self.id}, role={self.role}, content={self.content}, "
            f"status={self.status}, external_id={self.external_id})"
        )

    async def append_token(self, token: str, commit_interval: int = 2):
        """
        Append a token to the message.
        :param token: the token to append.
        """
        if self._data is None:
            raise Exception("The message is not in the context.")
        if self.content is None:
            self.content = ""
        self.content += token
        self._data["content"] = self.content
        await self._tokens.put(token)
        if len(self.content) % commit_interval == 0:
            await self._update_async(self._data)
            self._data = {}

    async def finish(self):
        """
        Finish the message.
        """
        await self._tokens.put(None)

    @property
    def id(self) -> str:
        return self._prisma_message.id

    @property
    def role(self) -> str | None:
        return self._prisma_message.role

    @property
    def content(self) -> str | None:
        return self._prisma_message.content

    @content.setter
    def content(self, value: str | None):
        if self._data is None:
            raise Exception("The message is not in the context.")
        self._data["content"] = value
        self._prisma_message.content = value

    @property
    def parent_id(self) -> str | None:
        return self._prisma_message.parent_id

    @parent_id.setter
    def parent_id(self, value: str | None):
        if self._data is None:
            raise Exception("The message is not in the context.")
        parent_payload: MessageUpdateOneWithoutRelationsInput = {}
        if value is None:
            parent_payload["disconnect"] = True
        else:
            parent_payload["connect"] = {"id": value}
        self._data["parent"] = parent_payload
        self._prisma_message.parent_id = value

    @property
    def status(self) -> str | None:
        return self._prisma_message.status

    @status.setter
    def status(self, value: str | None):
        if self._data is None:
            raise Exception("The message is not in the context.")
        self._data["status"] = value
        self._prisma_message.status = value

    @property
    def external_id(self) -> str | None:
        return self._prisma_message.external_id

    @external_id.setter
    def external_id(self, value: str):
        if self._data is None:
            raise Exception("The message is not in the context.")
        self._data["external_id"] = value
        self._prisma_message.external_id = value

    async def _update_async(
        self, data: MessageUpdateInput, condition: MessageWhereUniqueInput | None = None
    ):
        if condition is None:
            condition = {"id": self.id}
        await self._prisma_message.prisma().update(data=data, where=condition)
        result = await self._prisma_message.prisma().find_unique(where=condition)
        if result is not None:
            self._prisma_message = result


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
        if opt.role == SYSTEM:
            if parent is not None:
                raise Exception("The parent of a system message must be None.")
            if opt.content is None:
                raise Exception("The content of a system message cannot be None.")
        elif opt.role == USER:
            if parent is None:
                raise Exception("The parent of a user message cannot be None.")
            if opt.content is None:
                raise Exception("The content of a user message cannot be None.")
        elif opt.role == ASSISTANT:
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
                    "role": opt.role,
                    "content": opt.content,
                    "parent_id": parent_id,
                    "status": None,
                    "external_id": opt.external_id,
                }
            )
        )
        if opt.role == ASSISTANT:
            async with new_message as m:
                m.status = PENDING
            print(f"(core.append) After m.status = PENDING: {new_message}")
            task = asyncio.create_task(fake_api(new_message))
        self._messages[new_message.id] = new_message
        return new_message


async def fake_api(message: Message):
    async with message as m:
        # Update the properties of prism_message directly.
        print(f"(fake_api) Initial m: {m}")
        m.status = GENERATING
        print(f"(fake_api) After m.status=GENERATING: {m}")
        for i in range(10):
            await m.append_token(str(i))
            print(f"(fake_api) After append_token({i}): {m}")
            # await asyncio.sleep(1)
        await m.finish()
        print(f"(fake_api) After m.finish: {m}")
        m.status = FINISHED
        print(f"(fake_api) After m.status=FINISHED: {m}")
    # Write the properties of prism_message to the database.
    print(f"(fake_api) Final m: {m}")
