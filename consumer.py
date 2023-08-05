import random
import redis
import time
import json
import datetime

STREAM_KEY = "test_location"

r = redis.Redis(decode_responses=True)

last_job_id     = 0
job_fetch_count = 50
job_count       = 0
last_time_stamp = None

def addSecs(date_time, secs=5):
    return date_time + datetime.timedelta(seconds = secs)

while True:
    print("Checking for jobs...")
    response = r.xread(streams={STREAM_KEY: last_job_id}, count=job_fetch_count, block=5000)

    if len(response) == 0:
        print("Nothing to do right now, sleeping...")
        time.sleep(random.randint(5, 10))
    else:
        job = response[0]
        for location_data in job[1]:

            current_job_id = location_data[0]
            time_ms        = int(str(current_job_id)[2:-3])
            time_data      = datetime.datetime.fromtimestamp(time_ms/1000)

            current_job_details = location_data[1]
            job_count          += 1
            print(f"[Received] {job_count} time {time_data} for {current_job_details['idol']} => {current_job_details['lat']} && {current_job_details['lng']}")
            last_job_id     = current_job_id
            last_time_stamp = time_data
