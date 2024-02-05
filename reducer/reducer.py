
import redis
import os
from time import sleep

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]

r = redis.Redis(host=DB_HOST, port=DB_PORT, decode_responses=True)

N_MAPPERS = None

REDUCER_ID = r.incr("n_reducers")

p = r.pubsub()
p.subscribe(f"reducer-{REDUCER_ID}-input")

print("Reducer", REDUCER_ID, "subscribed")

n_message_received = 0

while N_MAPPERS is None or n_message_received < N_MAPPERS:
    reducer_signal = p.get_message(timeout=10,  ignore_subscribe_messages=True)
    if reducer_signal is not None:
        if N_MAPPERS is None:
            N_MAPPERS =  int(r.get("n_mappers"))
        n_message_received += 1  

inputs = []
for i in range(1, N_MAPPERS+1):
   a = r.hgetall(f"mapper-{i}-reducer-{REDUCER_ID}")
   print(type(a))
   inputs.append(a)


print(f"Reducer {REDUCER_ID} received {len(inputs)} inputs from mappers")

final_dictionnary = {}

for input in inputs:
    for word, count in input.items():
        if word in final_dictionnary:
            final_dictionnary[word] += int(count)
        else : 
            final_dictionnary[word] = int(count)

print(f"Reducer {REDUCER_ID} finished reducing, now sending data")

r.hset(f"reducer-{REDUCER_ID}", mapping=final_dictionnary)

r.publish("end", f"reducer-{REDUCER_ID} has finished")
