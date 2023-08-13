import random
import redis
import time
import json
import datetime

STREAM_KEY = "test_location"

r = redis.Redis(decode_responses=True)

last_job_id        = 0
job_fetch_count    = 10
job_count          = 0
last_time_stamp    = None
idol_loc_data      = dict()
idol_loc_data_temp = dict()

# calculateMidPoint() ->
# Used to calclate mid point from multiple location data.
def calculateMidPoint(LocationData):
    lat = []
    long = []
    for l in LocationData:
        lat.append(l[0])
        long.append(l[1])

    return [sum(lat)/len(lat), sum(long)/len(long)]

def addSecs(date_time, secs=5):
    return date_time + datetime.timedelta(seconds = secs)

def getTimeData(current_job_id):
    time_ms        = int(str(current_job_id)[2:-3])
    time_data      = datetime.datetime.fromtimestamp(time_ms/1000)
    return current_job_id, time_data

while True:
    idol_loc_data_temp.clear()
    print("Checking for jobs...")
    response = r.xread(streams={STREAM_KEY: last_job_id}, count=job_fetch_count, block=5000)

    if len(response) == 0:
        print("Nothing to do right now, sleeping...")
        time.sleep(random.randint(5, 10))
    else:
        job = response[0]
        for location_data in job[1]:
            current_job_id, time_data = getTimeData(location_data[0])

            current_job_details = location_data[1]
            job_count          += 1
            last_job_id     = current_job_id
            last_time_stamp = time_data
            if current_job_details['idol'] not in idol_loc_data_temp.keys():
                idol_loc_data_temp[current_job_details['idol']] = dict()
            idol_loc_data_temp[current_job_details['idol']][str(time_data)] = [float(current_job_details['lat']), float(current_job_details['lng'])]

        for idol in idol_loc_data_temp.keys():
            if idol not in idol_loc_data.keys():
                idol_loc_data[idol] = dict()

            loc = calculateMidPoint(idol_loc_data_temp[idol].values())
            idol_loc_data[idol][str(datetime.datetime.now())] = loc

        count = 0
        for idol in idol_loc_data.keys():
            count += len(idol_loc_data[idol])
            print(f"Idol : {idol} log count : {len(idol_loc_data[idol])}")

        print(f"Total Count : {count}")
        print(json.dumps(idol_loc_data, indent=2))

        time_to_sleep = random.randint(5, 6)
        print(f"Sleeping... for {time_to_sleep} Secs")
        time.sleep(time_to_sleep)
