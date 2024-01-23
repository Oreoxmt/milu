import asyncio
import time
import uuid
from asyncio import Queue
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import Dict, Type

from milu.db.prisma import Prisma
from milu.db.prisma.models import Message as PrismaMessage
from milu.db.prisma.types import (
    MessageUpdateInput,
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

    @asynccontextmanager
    async def append_token(self):
        token_queue: Queue[str | None] = Queue()

        async def task():
            content = ""
            token_count = 0
            commit_token_limit = 5
            commit_time_limit = 3
            db_last_update_time = time.monotonic()
            get_new_token = asyncio.create_task(token_queue.get())
            update_db_content = None
            pending = {get_new_token}
            while True:
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if get_new_token in done:
                    new_token = get_new_token.result()
                    if new_token is None:
                        await self._tokens.put(None)
                        return content
                    content += new_token
                    token_count += 1
                    await self._tokens.put(new_token)
                    get_new_token = asyncio.create_task(token_queue.get())
                    pending.add(get_new_token)
                if update_db_content in done:
                    update_db_content = None
                # Write the content of the message to the database every 3 seconds or 5 tokens.
                if update_db_content is None and (
                    time.monotonic() - db_last_update_time >= commit_time_limit
                    or token_count % commit_token_limit == 0
                ):
                    update_db_content = asyncio.create_task(
                        self._prisma_message.prisma().update(
                            data={"content": content},
                            where={"id": self.id},
                        )
                    )
                    pending.add(update_db_content)
                    db_last_update_time = time.monotonic()

        tasks = asyncio.create_task(task())
        try:
            yield token_queue
        finally:
            await token_queue.put(None)
            current_content = await tasks
            await self._update_async({"content": current_content}, None)

    @property
    def id(self) -> str:
        return self._prisma_message.id

    @property
    def role(self) -> str | None:
        return self._prisma_message.role

    @property
    def content(self) -> str | None:
        return self._prisma_message.content

    @property
    def parent_id(self) -> str | None:
        return self._prisma_message.parent_id

    @parent_id.setter
    def parent_id(self, value: str | None):
        if self._data is None:
            raise Exception("The message is not in the context.")
        if value is None:
            self._data["parent"] = {"disconnect": True}
        else:
            self._data["parent"] = {"connect": {"id": value}}

    @property
    def status(self) -> str | None:
        return self._prisma_message.status

    @status.setter
    def status(self, value: str | None):
        if self._data is None:
            raise Exception("The message is not in the context.")
        self._data["status"] = value

    @property
    def external_id(self) -> str | None:
        return self._prisma_message.external_id

    @external_id.setter
    def external_id(self, value: str):
        if self._data is None:
            raise Exception("The message is not in the context.")
        self._data["external_id"] = value

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
            task = asyncio.create_task(fake_api(new_message))
        self._messages[new_message.id] = new_message
        return new_message


async def fake_api(message: Message):
    async with message as mess_editor:
        mess_editor.status = GENERATING
        async with mess_editor.append_token() as q:
            for i in range(10):
                await q.put(str(i))
                await asyncio.sleep(1)
        mess_editor.status = FINISHED
    # Write the properties of prism_message to the database.
