from pymongo import MongoClient, errors as mongoError
from sys import argv
from time import time
import re

# global variables
time_limit = 120000  # max time in milliseconds


def printTime(func, *args):
    startTime = time()
    func(*args)
    timeTaken = time() - startTime

    print(f"Time taken: {timeTaken:.4f}s, {timeTaken*1000:.4f}ms")


def main(port):
    client = MongoClient("localhost", port)
    db = client["MP2Norm"]

    # make sure collections have been built
    if "messages" not in db.list_collection_names():
        print("Run task1_build.py then try again!")
        exit(1)

    if "senders" not in db.list_collection_names():
        print("Run task1_build.py then try again!")
        exit(1)

    # Collection instances
    messagesCollection = db["messages"]
    sendersCollection = db["senders"]

    messagesCollection.drop_indexes()
    sendersCollection.drop_indexes()

    # Run and time queries
    print("1. Return the number of messages that have \"ant\" in their text:")
    printTime(query1, messagesCollection)
    print()
    print("2. Find the sender who has sent the greatest number of messages:")
    printTime(query2, messagesCollection)
    print()
    print("3. Return the number of messages where the sender's credit is 0:")
    printTime(query3, sendersCollection)
    print()
    print("4. Double the credit of all senders whose credit is less than 100:")
    printTime(query4, sendersCollection)

    # indexing
    print("\nIndexing...\n")

    # create indexes for "sender" and "text" in messages
    messagesCollection.create_index({"sender": 1, "text": "text"})

    # create index for "sender_id" in senders
    sendersCollection.create_index({"sender_id": 1})

    # run and time queries after indexing
    print("Running queries 1, 2 & 3 after indexing...\n")
    print("1. Return the number of messages that have \"ant\" in their text:")
    printTime(query1, messagesCollection)
    print()
    print("2. Find the sender who has sent the greatest number of messages:")
    printTime(query2, messagesCollection)
    print()
    print("3. Return the number of messages where the sender's credit is 0:")
    printTime(query3, sendersCollection)


# Query 1
def query1(collection):
    # return number of messages that have ant in their text
    try:
        regx = re.compile("ant", re.IGNORECASE)
        result = list(collection.aggregate([
            {
                "$match": {"text": {"$regex": regx}}
            },
            {
                "$count": "count"
            }
        ], maxTimeMS=time_limit))

        print(result[0]["count"] if result else 0)
    except mongoError.ExecutionTimeout:
        print("Query takes more than 2 minutes.\nSkipping query...\n")


# Query 2
def query2(collection):
    # Find nickname/phone num of sender with highest texts and number of texts
    try:
        result = list(collection.aggregate([
            {
                "$group": {"_id": "$sender", "count": {"$count": {}}}
            },
            {
                "$sort": {"count": -1}
            },
            {
                "$limit": 1
            }
        ], maxTimeMS=time_limit))

        if result:
            nickname = result[0]["_id"]
            count = result[0]["count"]
            print(f'Nickname/Phone: {nickname}\nMessage Count: {count}')
        else:
            print("(empty)")
    except mongoError.ExecutionTimeout:
        print("Query takes more than 2 minutes.\nSkipping query...\n")


# Query 3
def query3(collection):
    # Return message count with 0 credit
    try:
        result = list(collection.aggregate([
            {
                "$match": {"credit": 0}
            },
            {
                "$lookup": {
                    "from": "messages",
                    "localField": "sender_id",
                    "foreignField": "sender",
                    "as": "sender_messages"
                }
            },
            # { "$unwind": {"path": "$sender_messages"} },
            # { "$count": "MessageCount" },
            {
                "$group": {
                    "_id": "null",
                    "MessageCount": {"$sum": {"$size": "$sender_messages"}},
                }
            }
        ], maxTimeMS=time_limit))

        messageCount = result[0]["MessageCount"] if result else 0
        print(messageCount)
    except mongoError.ExecutionTimeout:
        print("Query takes more than 2 minutes.\nSkipping query...\n")


# Query 4
def query4(collection):
    # double the credit of senders whose credit is less than 100
    try:
        collection.update_many(
            {"credit": {"$lt": 100}},
            {"$mul": {"credit": 2}}
        )
    except mongoError.ExecutionTimeout:
        print("Query takes more than 2 minutes.\nSkipping query...\n")


if __name__ == "__main__":
    if len(argv) != 2:
        print("Expected: python3 task1_query.py <port number>")
        exit(1)
    else:
        try:
            portNumber = int(argv[1])
        except ValueError:
            print("Invalid port number")
            exit(1)
        else:
            main(portNumber)
