from pymongo import MongoClient, collection, errors as mongoError
from sys import argv as args
from time import time
import re


def main(port: int):
    client = MongoClient("localhost", port)
    db = client["MP2Embd"]

    if "messages" not in db.list_collection_names():
        print("Call task2_build.py first!")
        exit(1)

    messagesCollection = db["messages"]

    print("1. Return the number of messages that have “ant” in their text:")
    printTime(numberOfTextWithAnt, messagesCollection)
    print()
    print("2. Find the sender who has sent the greatest number of messages:")
    printTime(senderWithHighestMessages, messagesCollection)
    print()
    print("3. Return the number of messages where the sender's credit is 0:")
    printTime(numberOfSendersWithZeroCredit, messagesCollection)
    print()
    print("4. Double the credit of all senders whose credit is less than 100")
    printTime(doubleCreditLessThan100, messagesCollection)


def printTime(func, *args):
    startTime = time()
    try:
        func(*args)
    except mongoError.ExecutionTimeout:
        print("Takes more than two minutes. Skipping...")
    except:
        print("(empty)")

    timeTaken = time() - startTime
    print(f"Time taken: {timeTaken:.4f}s, {timeTaken*1000:.4f}ms")


def numberOfTextWithAnt(collection: collection.Collection):
    regx = re.compile("ant", re.IGNORECASE)
    result = list(collection.aggregate([
        {
            "$match": {"text": {"$regex": regx}}
        },
        {
            "$count": "count"
        }
    ], maxTimeMS=120000))

    print(result[0]["count"] if result else 0)


def senderWithHighestMessages(collection: collection.Collection):
    result = list(collection.aggregate([
        {
            "$group": {"_id": "$sender", "messageCount": {"$count": {}}},
        },
        {
            "$sort": {"messageCount": -1}
        },
        {
            "$limit": 1
        }
    ], maxTimeMS=120000))

    if result:
        sender = result[0]["_id"]
        numberOfMessages = result[0]["messageCount"]
        print("Nickname/Phone:", sender)
        print("Message Count:", numberOfMessages)
    else:
        print("(empty)")


def numberOfSendersWithZeroCredit(collection: collection.Collection):
    print(collection.count_documents({"sender_info.credit": 0},
                                     maxTimeMS=120000))


def doubleCreditLessThan100(collection: collection.Collection):
    collection.update_many(
        {"sender_info.credit": {"$lt": 100}},
        {"$mul": {"sender_info.credit": 2}}
    )


if __name__ == "__main__":
    if len(args) != 2:
        print("Expected: python3 task2_query.py <port number>")
        exit(1)
    else:
        try:
            portNumber = int(args[1])
        except ValueError:
            print("Invalid port number")
            exit(1)
        else:
            main(portNumber)
