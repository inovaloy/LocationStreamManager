import redis
import json
from time import time
import datetime
from redis.exceptions import ConnectionError, DataError, NoScriptError, RedisError, ResponseError

redis_host = "localhost"
stream_key = "test_location"


def calculateMidPoint(L):
    lat = []
    long = []
    for l in L:
        lat.append(l[0])
        long.append(l[1])

    return sum(lat)/len(lat), sum(long)/len(long)

# Redis server setup & ping
r = redis.Redis( redis_host )
r.ping()

idol_details = dict()

location_data = r.xread( count=50, streams={stream_key:0
                                            } )

entry_count = 1

# print(json.dumps(location_data))
if(len(location_data) == 0):
    print("No data available!!!")
    exit
else:
    for loc in location_data[0][1]:
        time_stamp = (str(loc[0])[2:-1])
        # r.xdel(stream_key, time_stamp)
        print(time_stamp)

        if(loc[1][b'idol'] == b'undefined'):
            print("UnDEFINED VALUE !!! attempt to delete ", time_stamp)
            r.xdel(stream_key, time_stamp)
            continue
        else:
            print("(",entry_count,")")
            print("Idol : ", loc[1][b'idol'])

        if(loc[1][b'idol'] not in idol_details.keys()):
            idol_details[loc[1][b'idol']] = dict()
            idol_details[loc[1][b'idol']]['stream_loc'] = list()
            idol_details[loc[1][b'idol']]['stream_loc'].append([float(str(loc[1][b'lat'])[2:-1]), float(str(loc[1][b'lng'])[2:-1])])
            idol_details[loc[1][b'idol']]['stream_time'] = list()
            time_ms = int(str(loc[0])[2:-3])
            time = datetime.datetime.fromtimestamp(time_ms/1000)
            idol_details[loc[1][b'idol']]['stream_time'].append(time)
        else:
            idol_details[loc[1][b'idol']]['stream_loc'].append([float(str(loc[1][b'lat'])[2:-1]), float(str(loc[1][b'lng'])[2:-1])])
            time_ms = int(str(loc[0])[2:-3])
            time = datetime.datetime.fromtimestamp(time_ms/1000)
            idol_details[loc[1][b'idol']]['stream_time'].append(time)
        print("Lat  : ",loc[1][b'lat'])
        print("Long : ",loc[1][b'lng'])
        print("Time : ",time)
        print("\n")
        entry_count += 1

    print("REPORT : ")
    for idols in idol_details.keys():
        print("Idol : ", idols)
        lat, lng = calculateMidPoint(idol_details[idols]['stream_loc'])
        print("Lat  : ",round(lat, 7))
        print("Long : ",round(lng, 7))
        print("\n")