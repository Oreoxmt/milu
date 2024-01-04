import asyncio
import uuid
from asyncio import Queue
from dataclasses import dataclass
from typing import Any, Dict, Set

from milu.db.prisma import Prisma
from milu.db.prisma.models import Message as PrismaMessage

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
        self._tokens: Queue[str | None] = Queue()
        self._tasks: Set[asyncio.Task[Any]] = set()

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        next_token = await self._tokens.get()
        if next_token is None:
            raise StopAsyncIteration
        return next_token

    def __repr__(self):
        return (
            f"Message(id={self.id}, role={self.role}, content={self.content}, "
            f"status={self.status}, external_id={self.external_id})"
        )

    async def append_token(self, token: str):
        """
        Append a token to the message.
        :param token: the token to append.
        """
        if self.content is None:
            self.content = ""
            await self.await_tasks()
        self.content += token
        await self.await_tasks()
        await self._tokens.put(token)

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
        self._schedule_update({"content": value})

    @property
    def parent_id(self) -> str | None:
        return self._prisma_message.parent_id

    @parent_id.setter
    def parent_id(self, value: str | None):
        if value is None:
            data = {
                "parent": {
                    "disconnect": True,
                },
            }
        else:
            data = {
                "parent": {
                    "connect": {
                        "id": value,
                    },
                }
            }
        self._schedule_update(data)

    @property
    def status(self) -> str | None:
        return self._prisma_message.status

    @status.setter
    def status(self, value: str | None):
        self._schedule_update({"status": value})

    @property
    def external_id(self) -> str | None:
        return self._prisma_message.external_id

    @external_id.setter
    def external_id(self, value: str):
        self._schedule_update({"external_id": value})

    async def await_tasks(self):
        if len(self._tasks) > 0:
            await asyncio.gather(*self._tasks)
            self._tasks.clear()
            self._prisma_message = await self._prisma_message.prisma().find_unique(
                where={"id": self.id}
            )

    async def _update_prisma_message(
        self, data: dict[str, Any], condition: dict[str, Any] | None = None
    ):
        if condition is None:
            condition = {"id": self.id}
        new_message = await self._prisma_message.prisma().update(
            data=data, where=condition
        )
        if new_message is not None:
            self._prisma_message = new_message

    def _schedule_update(
        self, data: dict[str, Any], condition: dict[str, Any] | None = None
    ):
        task = asyncio.create_task(self._update_prisma_message(data, condition))
        self._tasks.add(task)


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
            new_message.status = PENDING
            task = asyncio.create_task(fake_api(new_message))
        await new_message.await_tasks()
        self._messages[new_message.id] = new_message
        return new_message


async def fake_api(message: Message):
    message.status = GENERATING
    for i in range(10):
        await message.append_token(str(i))
        await asyncio.sleep(1)
    await message.finish()
    message.status = FINISHED
    await message.await_tasks()
