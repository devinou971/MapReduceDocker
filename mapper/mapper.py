
import redis
import os
import re
from collections import Counter
import hashlib
import socket


DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
RESUMING_OLD_PROCESS = False


def word_occurrences(text):
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
    hash_val = int(hashlib.sha256(word.encode()).hexdigest(), 16)
    return hash_val % num_reducers


r = redis.Redis(host=DB_HOST, port=DB_PORT, decode_responses=True, socket_connect_timeout=15)

# Here, we get the mapper ID from the Redis DB
hostname = socket.gethostname()
print("Hostname:", hostname)

MAPPER_ID = hostname

all_mappers = r.lrange("mappers", 0, -1)
if hostname not in all_mappers:
    print("Adding new mapper")
    r.lpush("mappers", hostname)

print("Mapper", MAPPER_ID, "started")

mapper_subtext_ids = r.get(f"input-{MAPPER_ID}")
if mapper_subtext_ids is None:
    p = r.pubsub()
    p.subscribe(f"input-{MAPPER_ID}")

    print("Mapper", MAPPER_ID, "subscribed")

    mapper_input = p.get_message(timeout=10,  ignore_subscribe_messages=True)
    while mapper_input is None:
        mapper_input = p.get_message(timeout=10,  ignore_subscribe_messages=True)

    mapper_subtext_ids = mapper_input["data"]

start = int(mapper_subtext_ids.split(" ")[0])
end = int(mapper_subtext_ids.split(" ")[1])
mapper_text = r.substr("full-text", start, end)

print("Mapper", MAPPER_ID, "got data of length:", len(mapper_text))

mapping_result = word_occurrences(mapper_text)
print("Mapper", MAPPER_ID, "finished mapping, now sending to reducers")

REDUCERS = r.lrange("reducers", 0, -1)
N_REDUCERS = len(REDUCERS)

results_to_send = [{} for _ in range(N_REDUCERS)]

for word, count in mapping_result.items():
    r_id = get_reducer_key(word, N_REDUCERS)
    results_to_send[r_id][word] = count

for i in range(N_REDUCERS):
    r.hset(f"input-{REDUCERS[i]}-from-{MAPPER_ID}", mapping=results_to_send[i])
    r.publish(f"input-{REDUCERS[i]}", f"upload finished from mapper {MAPPER_ID}")

print(f"Mapper {MAPPER_ID} finished its job")


while True:
    pass