from pymongo import AsyncMongoClient
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")


async def main():
    client = AsyncMongoClient(MONGO_URL)

    # Optional: establish connection eagerly
    await client.aconnect()

    # Run ping command
    result = await client.admin.command("ping")
    print("Ping result:", result)

    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
