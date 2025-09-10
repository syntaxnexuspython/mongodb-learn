import asyncio
import os
from dotenv import load_dotenv
from pymongo import AsyncMongoClient
from pymongo.server_api import ServerApi
import datetime
from faker import Faker
from bson import ObjectId
import random

load_dotenv()

fake = Faker()


async def ping_server() -> None:
    uri = os.getenv("MONGO_URL")
    client = AsyncMongoClient(uri, server_api=ServerApi("1"))
    try:
        db = client.learn
        posts = db.posts
        # post = {
        #     "author": fake.name(),
        #     "text": fake.text(100),
        #     "tags": ["mongodb", "python", "pymongo"],
        #     "native_language": fake.language_name(),
        #     "date": datetime.datetime.now(tz=datetime.timezone.utc),
        # }

        # post_id = (await posts.insert_one(post)).inserted_id
        # print('post_id',post_id)
        # ADD MULTIPLE DATA

        # data = []
        # for _ in range(5):
        #     data.append({
        #         "author": fake.name(),
        #         "text": fake.text(100),
        #         "native_language": fake.language_name(),
        #         "date": datetime.datetime.now(tz=datetime.timezone.utc),

        #     })
        # post_ids = (await posts.insert_many(data)).inserted_ids
        # print('collection successfully created',post_ids)

        # FIND METHOD
        # print(await posts.find_one({'_id': ObjectId("68c148c6b698173ba717f4ec")}))
        # EXCLUDE 'date'
        # print('Exclude the date in result')
        # print(await posts.find_one({'_id': ObjectId("68c148c6b698173ba717f4ec")}, {'date': False}))

        # FIND MULTIPLE RECORDS
        # cursor = posts.find({'native_language': 'Icelandic'}).skip(0).limit(5)
        # docs = await cursor.to_list(length=None)  # fetch all
        # print(docs)

        # INSERTED DOCUMENT IN USER
        # users = await db.create_collection('users')
        # print('users document created successfully')

        users = db.users
        # male_user_id = (
        #     await users.insert_one(
        #         {"name": fake.name_male(), "gender": "Male", "age": 25}
        #     )
        # ).inserted_id

        # female_user_id = (
        #     await users.insert_one(
        #         {"name": fake.name_female(), "gender": "Female", "age": 20}
        #     )
        # ).inserted_id

        # Update user document

        # await users.update_one(
        #     {"_id": male_user_id},
        #     {"$set": {"country": "India", "state": "TN", "district": "KK"}},
        # )
        # await users.update_one(
        #     {"_id": female_user_id},
        #     {
        #         "$set": {
        #             "country": "Australia",
        #             "state": "Somewhere",
        #             "district": "Wherever",
        #         }
        #     },
        # )
        # result_cursor = users.find({"_id": {"$in": [male_user_id, female_user_id]}})
        # print(await result_cursor.to_list(length=None))

        # Delete User document

        # await users.delete_one({"_id": male_user_id})

        # print("\nAfter deleting male user id")
        # result_cursor = users.find({"_id": {"$in": [male_user_id, female_user_id]}})
        # print(await result_cursor.to_list(length=None))

        # delete all user documents

        # await users.delete_many({})

        # print("\nAfter deleting all documents")
        # result_cursor = users.find()
        # print(await result_cursor.to_list(length=None))

        # await users.delete_many({})

        data = []

        for i in range(100):
            if i % 2 == 0:
                data.append(
                    {
                        "name": fake.name_male(),
                        "gender": "Male",
                        "age": random.randint(10, 60),
                        "visited_country": [
                            fake.country() for _ in range(random.randint(1, 5))
                        ],
                        "address": {
                            "address_line1": fake.street_address(),
                            "zip_code": fake.postalcode_in_state(),
                            "state": fake.state(),
                            "country": fake.country()
                        },
                    }
                )
            else:
                data.append(
                    {
                        "name": fake.name_female(),
                        "gender": "Female",
                        "age": random.randint(10, 60),
                        "visited_country": [
                            fake.country() for _ in range(random.randint(1, 5))
                        ],
                        "address": {
                            "address_line1": fake.street_address(),
                            "zip_code": fake.postalcode_in_state(),
                            "state": fake.state(),
                            "country": fake.country()
                        },
                    }
                )

        await users.insert_many(data)

        cursor = users.find().batch_size(100)

        async for doc in cursor:
            print(doc)

        # Update Embedded documents
        # update_user_id = ObjectId('68c16f5eca2cbb4fb454f5f9')
        # await users.update_one({"_id": update_user_id}, {
        #     "$set": {'address.address_line1': "Somewhere"}
        # })

        # print( await users.find_one({'_id': update_user_id}))

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(ping_server())

