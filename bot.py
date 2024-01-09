import asyncio

from milu.core import AppendOption, Core

USER = "user"
ASSISTANT = "assistant"
SYSTEM = "system"


async def main() -> None:
    core = Core()
    db_prisma = core._client
    print("(bot) Connecting to database...")
    await db_prisma.connect()
    print("(bot) Deleting all messages...")
    await db_prisma.message.delete_many()
    system_message = await core.append(
        None,
        AppendOption(role=SYSTEM, content="You are a helpful assistant."),
    )
    print(system_message)
    user_message = await core.append(
        system_message,
        AppendOption(role=USER, content="Count from 0 to 9."),
    )
    print(user_message)
    assistant_message = await core.append(user_message, AppendOption(role=ASSISTANT))
    print(assistant_message)
    async for token in assistant_message:
        print(f"(bot) The newly generated token is: {token}")
    await assistant_message.await_tasks()
    print(assistant_message)
    await db_prisma.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
