import asyncio

from core import AppendOption, Core, MessageRole


async def main() -> None:
    core = Core()
    db_prisma = core._client
    print("Connecting to database...")
    await db_prisma.connect()
    # print("Deleting all messages...")
    # await db_prisma.message.delete_many()
    system_message = await core.append(
        None,
        AppendOption(role=MessageRole.SYSTEM, content="You are a helpful assistant."),
    )
    await system_message.create()
    print(system_message)
    user_message = await core.append(
        system_message,
        AppendOption(role=MessageRole.USER, content="Count from 0 to 9."),
    )
    await user_message.create()
    print(user_message)
    assistant_message = await core.append(
        user_message, AppendOption(role=MessageRole.ASSISTANT)
    )
    await assistant_message.create()
    print(assistant_message)
    for token in assistant_message:
        print(f"Current status: {assistant_message.status}")
        print(f"The newly generated token is: {token}")
        print(f"The content is: {assistant_message.content}")
        await assistant_message.update({"content": assistant_message.content})
    print(f"Current status: {assistant_message.status}")
    await assistant_message.update({"status": assistant_message.status.value})
    print(assistant_message)
    await db_prisma.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
