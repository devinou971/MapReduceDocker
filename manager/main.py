import redis
import os
from sys import argv

print("Manager started")

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
MAPPER_IDS = []
REDUCER_IDS = []


def get_text_splits(text:str, num_parts:int):
        """
           Calculate the offsets to split the text into num_parts parts.
           The split appens between words.

           text: the text to split
           num_parts: the number of parts to split the text into
           returns: a list of strings, each string is of the form "<start offset> <end offset>"
        """
        parts = []

        nb_carac = len(text) // num_parts

        start_offset = 0
        for i in range(num_parts-1):
            offset = 0
            ch = ''

            while ch != ' ':
                offset += 1
                ch = text[(i+1)*nb_carac+offset]

            end_offset = (i+1)*nb_carac + offset
            
            parts.append(f"{start_offset} {end_offset}")
            start_offset = end_offset

        parts.append(f"{start_offset} {len(text)}")
        
        return parts

r = redis.Redis(host=DB_HOST, port=DB_PORT, decode_responses=True, socket_connect_timeout=15)




p = r.pubsub()
p.subscribe("start")

while True:
    n_mappers = 0
    n_reducers = 0

    
    file_name = None

    while n_mappers == 0 or n_reducers == 0:

        # Waiting for the start signal
        print("Waiting for start signal ...")
        response = p.get_message(timeout=10, ignore_subscribe_messages=True)
        while response is None:
            response = p.get_message(timeout=10, ignore_subscribe_messages=True)
        
        file_name = response["data"]

        # Waiting for mappers and reducers 
        MAPPER_IDS = r.lrange("mappers", 0, -1)
        REDUCER_IDS = r.lrange("reducers", 0, -1)

        n_mappers = len(MAPPER_IDS)
        n_reducers = len(REDUCER_IDS)

        if n_mappers == 0 or n_reducers == 0:
            print(f"Not enough mappers or reducers to start (n_mappers = {n_mappers}, n_reducers = {n_reducers})")

    if not os.path.exists(os.path.join("/data/", file_name)):
        print("File not found")
        continue

    with open(os.path.join("/data/", file_name) , "r") as f:
        file_content = f.read()

    r.set("full-text", file_content)

    # Telling mappers and reducers to be ready
    r.set("map-reduce-started", 1)
    r.publish("map-reduce-started", 1)

    
    # Splitting the file for the mappers
    print(f"Splitting the file for n_mappers = {n_mappers}")
    file_splits = get_text_splits(file_content, n_mappers)

    # Sending the splits to the mappers
    print(f"Sending the splits to the mappers")
    for i, mapper_id in enumerate(MAPPER_IDS):
        print("Manager sending to mapper", mapper_id, "data with length", len(file_splits[i]))
        r.set(f"input-{mapper_id}", file_splits[i])
        r.publish(f"input-{mapper_id}", file_splits[i])

    p.unsubscribe("start")

    # Waiting for the reducers to finish
    p.subscribe("end")
    n_received_message = 0
    while n_received_message < n_reducers:
        reducer_end_messages = p.get_message(timeout=10,  ignore_subscribe_messages=True)
        if reducer_end_messages is not None:
            n_received_message += 1

    # Aggregating the results
    reducer_outputs = r.keys("output-*")
    final_output = {}
    for output_id in reducer_outputs:
        output = r.hgetall(output_id)
        final_output.update(output)

    r.hset("final-output", mapping=final_output)

    # Cleaning up
    for i in reducer_outputs+r.keys("input-*"):
        r.delete(i)