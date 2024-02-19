
import redis
import os
import re
from collections import Counter
import hashlib
import socket

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]

# Here, we get the mapper ID from the Redis DB
MAPPER_ID = socket.gethostname()
print("Hostname:", MAPPER_ID)

r = redis.Redis(host=DB_HOST, port=DB_PORT, decode_responses=True, socket_connect_timeout=15)

def word_occurrences(text):
    """
        Calculate the number of occurrences of each word in the text.
        text: the text to process
        results: a dictionary with the word as key and the number of occurrences as value
    """
    word_pattern = re.compile(r'\b\w+\b')
    words = [word.lower() for word in word_pattern.findall(text)]

    punctuation_pattern = re.compile(r'[^\w\s]')
    punctuations = [punctuation for punctuation in punctuation_pattern.findall(text)]

    word_counts = Counter(words)
    punctuation_counts = Counter(punctuations)

    result = dict(word_counts)
    result.update(punctuation_counts)

    return result

def get_reducer_key(word, num_reducers):
    """
        Finds the reducer that will process the word.
        word: the word to calculate the reducer ID for
        num_reducers: the number of reducers
        returns: the reducer ID
    """
    hash_val = int(hashlib.sha256(word.encode()).hexdigest(), 16)
    return hash_val % num_reducers


# Add the mapper to the list of mappers
all_mappers = r.lrange("mappers", 0, -1)
if MAPPER_ID not in all_mappers:
    print("Adding new mapper")
    r.lpush("mappers", MAPPER_ID)


while True:
    print("Mapper", MAPPER_ID, "waiting for signal")

    # Wait for the signal to start the pipeline
    pipeline_already_started = r.get("map-reduce-started")
    pipeline_already_started = 0 if pipeline_already_started else pipeline_already_started
    number_of_outputs = len(r.keys(f"*from-{MAPPER_ID}"))
    REDUCERS = r.lrange("reducers", 0, -1)
    if pipeline_already_started == 0 or (pipeline_already_started == 1 and number_of_outputs == len(REDUCERS)):
        p_start = r.pubsub()
        p_start.subscribe("map-reduce-started")
        res = p_start.get_message(timeout=10, ignore_subscribe_messages=True)
        while res is None:
            res = p_start.get_message(timeout=10, ignore_subscribe_messages=True)

    print("Mapper", MAPPER_ID, "started")

    print("Mapper", MAPPER_ID, "waiting for data")

    # Get the offsets of the subtexts for this mapper
    mapper_subtext_ids = r.get(f"input-{MAPPER_ID}")
    if mapper_subtext_ids is None:
        p = r.pubsub()
        p.subscribe(f"input-{MAPPER_ID}")
        mapper_input = p.get_message(timeout=10,  ignore_subscribe_messages=True)
        while mapper_input is None:
            mapper_input = p.get_message(timeout=10,  ignore_subscribe_messages=True)
        mapper_subtext_ids = mapper_input["data"]

    start_offset = int(mapper_subtext_ids.split(" ")[0])
    end_offset = int(mapper_subtext_ids.split(" ")[1])
    mapper_text = r.substr("full-text", start_offset, end_offset)

    print("Mapper", MAPPER_ID, "got data")

    # Map the text
    mapping_result = word_occurrences(mapper_text)
    print("Mapper", MAPPER_ID, "finished mapping, now sending to reducers")

    # Send the results to the reducers
    REDUCERS = r.lrange("reducers", 0, -1)
    results_to_send = [{} for _ in range(len(REDUCERS))]

    for word, count in mapping_result.items():
        r_id = get_reducer_key(word, len(REDUCERS))
        results_to_send[r_id][word] = count

    for i in range(len(REDUCERS)):
        r.hset(f"input-{REDUCERS[i]}-from-{MAPPER_ID}", mapping=results_to_send[i])
        r.publish(f"input-{REDUCERS[i]}", f"upload finished from mapper {MAPPER_ID}")

    print(f"Mapper {MAPPER_ID} finished its job")
