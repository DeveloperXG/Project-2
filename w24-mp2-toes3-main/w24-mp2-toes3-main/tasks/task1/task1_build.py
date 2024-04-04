from pymongo import MongoClient
import json
from time import time
import sys


def main(port):
    client = MongoClient("localhost", port)

    # Creates a db called MP2Embd
    db = client.MP2Norm

    # If messages collection already exists, delete it and create again
    if "messages" in db.list_collection_names():
        db.messages.drop()

    messages_collection = db.messages

    # If senders collection already exists, delete it and create again
    if "senders" in db.list_collection_names():
        db.senders.drop()

    senders_collection = db.senders

    # ----------------------------------------------------------------
    # Running time for reading and insertion of documents into messages_collection
    print("Building 'messages' collection...", flush=True, end="")
    m_start_time = time()

    # Initialize a batch array and batch size
    batch = []
    batch_size = 5000

    with open("messages.json", 'r') as file:
        # Makes sure each line in the file is read
        for line in file:
            jsonLine = line.strip()
            """
                Since we're parsing line by line, we want to avoid
                - The list delimiter (i.e., '[' or ']')
                - Commas between items in the list
            """
            if jsonLine in "[]":
                continue
            elif jsonLine[-1] == ',':
                jsonLine = jsonLine[0:-1]

            # Parses the JSON string and appends it to batch array
            message = json.loads(jsonLine)
            batch.append(message)

            # If len(batch) >= 5000, insert it into collection, reset the batch, and start again
            if len(batch) >= batch_size:
                try:
                    messages_collection.insert_many(batch)
                    batch = []
                except Exception as e:
                    print("Error:", e)
                    exit(1)

    # Inserts the remaining documents
    if batch:
        messages_collection.insert_many(batch)

    m_end_time = time()
    m_time_taken = m_end_time - m_start_time
    print(f"Done in {m_time_taken:.4f}s ({m_time_taken*1000:.4f}ms)")
    # ---------------------------------------------------------------
    # Running time for reading and insertion of documents into senders_collection
    print("Building 'senders' collection...", flush=True, end="")
    s_start_time = time()

    # utf-8 encoding needed for special characters
    with open("senders.json", 'r', encoding="utf-8") as file2:
        # Reads the entire content of the file as a single string
        data = json.load(file2)

    # insert_many() for senders.json file can be done in one operation
    try:
        senders_collection.insert_many(data)
    except Exception as e:
        print("Error:", e)
        exit(1)

    s_end_time = time()
    s_time_taken = s_end_time - s_start_time
    print(f"Done in {s_time_taken:.4f}s ({s_time_taken*1000:.4f}ms)")
    # ---------------------------------------------------------------

    total_time = m_time_taken + s_time_taken
    print(f"Total: {total_time:.4f}s ({total_time*1000:.4f}ms)")


if __name__ == "__main__":
    # Check for correct argument structure
    if len(sys.argv) != 2:
        print("Expected: python3 task1_build.py <port number>")
        exit(1)

    port = int(sys.argv[1])
    main(port)
