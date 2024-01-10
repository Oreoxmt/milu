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
    print(f"(bot) system_message: {system_message}")
    user_message = await core.append(
        system_message,
        AppendOption(role=USER, content="Count from 0 to 9."),
    )
    print(f"(bot) user_message: {user_message}")
    assistant_message = await core.append(user_message, AppendOption(role=ASSISTANT))
    print(f"(bot) assistant_message before generating token: {assistant_message}")
    async for token in assistant_message:
        print(f"(bot) Generated token: {token}")
    print(f"(bot) assistant_message after generating token: {assistant_message}")
    await db_prisma.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
