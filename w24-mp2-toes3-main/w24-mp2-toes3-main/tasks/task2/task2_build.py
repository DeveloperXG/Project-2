from pymongo import MongoClient
import json
from time import time
import sys


def main(port):
    # Initialize MongoDB instance and database
    client = MongoClient("localhost", port)
    db = client.MP2Embd

    # Drop messages collection if it exists
    if 'messages' in db.list_collection_names():
        db.messages.drop()

    messages_collection = db.messages

    startTime = time()
    print("Building collection...", flush=True, end="")

    # Load senders.json into memory
    sendersInfo = dict()
    with open('senders.json', 'r') as file:
        # We get a list of (sender_id, name, credit)
        senders_data = json.load(file)
        # Convert list to dictionary
        for sender in senders_data:
            sendersInfo[sender["sender_id"]] = sender

    # process messages.json and insert to messages_collection
    batch = []
    batch_size = 5000
    with open('messages.json', 'r') as file:
        lineCounter = 0
        # Read file line by line to avoid storing in memory
        for line in file:
            json_line = line.strip()
            if json_line[-1] == ',':
                json_line = json_line[:-1]

            lineCounter += 1
            # Skip first and last line
            if json_line in "[]":
                continue
            try:
                message: dict = json.loads(json_line)
            except json.JSONDecodeError as e:
                print(e)
                print("Happened on line", lineCounter)
                exit(1)

            # Embed sender info to messages
            sender_id = message.get('sender')
            if sender_id:
                # Create new field in message that contains the sender's info
                message['sender_info'] = sendersInfo[sender_id]
                # Add to batch
                batch.append(message)

            # Insert small batch
            if len(batch) >= batch_size:
                messages_collection.insert_many(batch)
                batch = []

    # Insert any remaining messages
    if batch:
        messages_collection.insert_many(batch)

    timeTaken = time()-startTime
    print(f"Done in {timeTaken:.4f}s ({timeTaken * 1000:.4f}ms)")


if __name__ == "__main__":
    # check for correct argument structure
    if len(sys.argv) != 2:
        print("Expected: python3 task2_build.py <port number>")
        sys.exit(1)
    else:
        port = int(sys.argv[1])
        main(port)
