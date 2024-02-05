import redis
import os
from sys import argv

print("Manager started")

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
r = redis.Redis(host=DB_HOST, port=DB_PORT, decode_responses=True, socket_connect_timeout=15)

file_content = ""
with open(argv[1], "r") as f:
    file_content = f.read()

r.set("full-text", file_content)

p = r.pubsub()
p.subscribe("start")


n_mappers = 0
n_reducers = 0

# Wait for mappers and reducers 

while n_mappers == 0 or n_reducers == 0:
    print("Waiting for start signal ...")
    res = p.get_message(timeout=10, ignore_subscribe_messages=True)
    while res is None:
        res = p.get_message(timeout=10, ignore_subscribe_messages=True)

    n_mappers = int(r.get("n_mappers"))
    n_reducers = int(r.get("n_reducers"))

    if n_mappers == 0 or n_reducers == 0:
        print(f"Not enough mappers or reducers to start (n_mappers = {n_mappers}, n_reducers = {n_reducers})")

def get_text_splits(text:str, num_parts:int):
    parts = []

    nb_carac = len(text) // num_parts

    last_index = 0
    for i in range(num_parts-1):
        offset = 0
        ch = ''

        while ch != ' ':
            offset += 1
            ch = text[(i+1)*nb_carac+offset]

        end_part_index = (i+1)*nb_carac + offset
        
        parts.append(f"{last_index} {end_part_index}")
        last_index = end_part_index

    parts.append(f"{last_index} {len(text)}")
    
    return parts

print(f"Splitting the file for n_mappers = {n_mappers}")
# file_splits = split_text(file_content, n_mappers)
file_splits = get_text_splits(file_content, n_mappers)
print(file_splits)

print(f"Sending the splits to the mappers")
for i in range(n_mappers):
    print("Manager sending to mapper", i+1, "data with length", len(file_splits[i]))
    r.publish(f"mapper_{i+1}_input", file_splits[i])

p.unsubscribe("start")
p.subscribe("end")
n_received_message = 0

while n_received_message< n_reducers:
    reducer_end_messages = p.get_message(timeout=10,  ignore_subscribe_messages=True)
    if reducer_end_messages is not None:
        n_received_message += 1

