import random
import redis
import time
import json
import datetime
import mysql.connector

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

def connectMySQL():
    mydb = mysql.connector.connect(host="host_name", user="user_name", password="password", database="database_name")
    return mydb, mydb.cursor()

def insertDataIntoDataBase(data):
    if len(data) == 0:
        print("Data can't be empty")
        return False
    print(json.dumps(data, indent=4))

    data_value = list()

    for idol in data:
        time_data = list(data[idol].keys())[0]
        if len(data[idol][time_data]) != 2:
            print("Data should be of length 2")
            return False

        lat = data[idol][time_data][0]
        lng = data[idol][time_data][1]
        if (not isinstance(lat, float)) or (not isinstance(lng, float)):
            print("Data should be float")
            return False

        data_value.append([time_data, lat, lng, idol])

    insert_query = "INSERT INTO location_tracker (time, lat, lng, idol_name) VALUES (%s, %s, %s, %s)"
    update_query = "UPDATE `latest_location` SET time=%s, lat=%s, lng=%s WHERE idol_name=%s"

    mydb, mycursor = connectMySQL()

    mycursor.executemany(insert_query, data_value)
    mydb.commit()
    print("Data added")

    mycursor.executemany(update_query, data_value)
    mydb.commit()
    print("Latest Location updated")


while True:
    idol_loc_data_temp.clear()
    idol_loc_data.clear()
    print("Checking for jobs...")
    response = r.xread(streams={STREAM_KEY: last_job_id}, count=job_fetch_count, block=5000)

    if len(response) == 0:
        print("Nothing to do right now, sleeping...")
        time.sleep(random.randint(5, 6))
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

        insertDataIntoDataBase(idol_loc_data)

        count = 0
        for idol in idol_loc_data.keys():
            count += len(idol_loc_data[idol])

        time_to_sleep = random.randint(5, 6)
        print(f"Sleeping... for {time_to_sleep} Secs")
        time.sleep(time_to_sleep)
