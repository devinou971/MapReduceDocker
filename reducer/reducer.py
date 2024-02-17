
import redis
import os
import socket

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
REDUCER_ID = socket.gethostname()

r = redis.Redis(host=DB_HOST, port=DB_PORT, decode_responses=True)


while True:
    
    all_reducers = r.lrange("reducers", 0, -1)
    if REDUCER_ID not in all_reducers:
        print("Adding new reducer")
        r.lpush("reducers", REDUCER_ID)

    print("Reducer", REDUCER_ID, "waiting for signal")

    pipeline_already_started = r.get("map-reduce-started")
    pipeline_already_started = 0 if pipeline_already_started else pipeline_already_started
    number_of_outputs = len(r.keys(f"output-{REDUCER_ID}"))
    if pipeline_already_started == 0 or (pipeline_already_started == 1 and number_of_outputs == 1):
        p_start = r.pubsub()
        p_start.subscribe("map-reduce-started")
        res = p_start.get_message(timeout=10, ignore_subscribe_messages=True)
        while res is None:
            res = p_start.get_message(timeout=10, ignore_subscribe_messages=True)
    
    print("Reducer", REDUCER_ID, "waiting for mapped results")


    MAPPERS = r.lrange("mappers", 0, -1)

    p = r.pubsub()
    p.subscribe(f"input-{REDUCER_ID}")
    print("Reducer", REDUCER_ID, "subscribed")
    print("Waiting for mappers to start")
    n_message_received = 0

    while len(MAPPERS) == 0 or n_message_received < len(MAPPERS):
        reducer_signal = p.get_message(timeout=10,  ignore_subscribe_messages=True)
        if reducer_signal is not None:
            MAPPERS = r.lrange("mappers", 0, -1)
            n_message_received += 1  


    inputs = []
    all_keys = r.keys(f"input-{REDUCER_ID}-from-*")
    for key in all_keys:
        res = r.hgetall(key)
        inputs.append(res)


    print(f"Reducer {REDUCER_ID} received {len(inputs)} inputs from mappers")

    final_dictionnary = {}

    for input in inputs:
        for word, count in input.items():
            if word in final_dictionnary:
                final_dictionnary[word] += int(count)
            else : 
                final_dictionnary[word] = int(count)

    print(f"Reducer {REDUCER_ID} finished reducing, now sending data")

    r.hset(f"output-{REDUCER_ID}", mapping=final_dictionnary)

    r.publish("end", f"reducer-{REDUCER_ID} has finished")

