
import redis
import os
import re
from collections import Counter
import hashlib

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]

r = redis.Redis(host=DB_HOST, port=DB_PORT, decode_responses=True, socket_connect_timeout=15)

MAPPER_ID = r.incr("n_mappers")

p = r.pubsub()
p.subscribe(f"mapper_{MAPPER_ID}_input")

print("Mapper", MAPPER_ID, "subscribed")


mapper_input = p.get_message(timeout=10,  ignore_subscribe_messages=True)
while mapper_input is None:
    mapper_input = p.get_message(timeout=10,  ignore_subscribe_messages=True)

mapper_subtext_ids = mapper_input["data"]

start = int(mapper_subtext_ids.split(" ")[0])
end = int(mapper_subtext_ids.split(" ")[1])
mapper_text = r.substr("full-text", start, end)

print("Mapper", MAPPER_ID, "got data of length:", len(mapper_text))

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

mapping_result = word_occurrences(mapper_text)

print("Mapper", MAPPER_ID, "finished mapping, now sending to reducers")

N_REDUCERS = int(r.get("n_reducers"))

results_to_send = [{} for _ in range(N_REDUCERS)]

for word, count in mapping_result.items():
    r_id = get_reducer_key(word, N_REDUCERS)
    results_to_send[r_id][word] = count

for i in range(N_REDUCERS):
    r.hset(f"mapper-{MAPPER_ID}-reducer-{i+1}", mapping=results_to_send[i])
    r.publish(f"reducer-{i+1}-input", f"upload finished from mapper {MAPPER_ID}")

print(f"Mapper {MAPPER_ID} finished its job")