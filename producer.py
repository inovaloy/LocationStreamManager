import random
import redis
import time

STREAM_KEY = "test_location"

IDOL_LIST = [
    "Buri  Maa",
    "Choto Maa",
    "Amar  Maa",
    "Mejo  Maa"
]

r = redis.Redis(decode_responses=True)

while True:
    job = {
        "idol": random.choice(IDOL_LIST),
        "lat": random.uniform(12.00000000, 12.9999999),
        "lng": random.uniform(17.00000000, 17.9999999),
    }

    job_id = r.xadd(STREAM_KEY, job)
    print(f"Created job {job_id}:{job}")

    time.sleep(random.randint(2, 20))