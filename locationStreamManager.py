import redis
import datetime
import time
import random
from redis.exceptions import ConnectionError, DataError, NoScriptError, RedisError, ResponseError


class locationStreamManager:

    # Common connection variables
    REDIS_HOST      = "localhost"
    STREAM_KEY      = "test_location"
    redisConnection = None

    def __init__(self,
                 host_name = None,
                 stream_key = None,
                 redis_connection = None):
        # init connection from another connection
        if redis_connection and self.isConnectionAlive(redis_connection):
            self.redisConnection = redis_connection
        else:
            # init connection by default
            if host_name:
                self.REDIS_HOST = host_name
            if stream_key:
                self.STREAM_KEY = stream_key
            self.clientConnect()


    # calculateMidPoint() ->
    # Used to calclate mid point from multiple location data.
    def calculateMidPoint(self, LocationData):
        lat = []
        long = []
        for l in LocationData:
            lat.append(l[0])
            long.append(l[1])

        return sum(lat)/len(lat), sum(long)/len(long)

    # clientConnect() ->
    # Used to establish connection with redis server.
    def clientConnect(self):
        # Redis server setup & ping
        self.redisConnection = redis.Redis(self.REDIS_HOST, socket_connect_timeout=1)
        try:
            print(f"Connection status : {self.redisConnection.ping()}")
        except:
            print("Connection failed !!!")
            return False
        return True

    # getAllStreamData() ->
    # Used to get all stream of data for a particular time.
    def getAllStreamData(self):
        # Check connection status
        if not self.isConnectionAlive():
            return False

        location_data = self.redisConnection.xread(count=50, streams={self.STREAM_KEY:0})
        if location_data:
            return location_data
        else:
            return False

    # getStreamData() ->
    # Used to get stream of data for a particular time.
    def getStreamData(self, last_job_id):
        # Check connection status
        if not self.isConnectionAlive():
            return False

        if last_job_id < 0:
            print("Last job ID can't be negetive !!!")
            return False

        print("Checking for jobs...")
        location_data = self.redisConnection.xread(count=50, streams={self.STREAM_KEY:last_job_id}, block=5000)

        if len(location_data) == 0:
            print("Nothing to do right now, sleeping...")
            time.sleep(random.randint(5, 10))

        if location_data:
            return location_data
        else:
            return False

    # isConnectionAlive() ->
    # Used to check is the connection available or not for redis_connection or the default redisConnection.
    def isConnectionAlive(self, redis_connection = None):
        if redis_connection:
            try:
                return redis_connection.ping()
            except:
                print("Connection lost !!!")
                return False
        else:
            try:
                return self.redisConnection.ping()
            except:
                print("Connection lost !!!")
                return False

    # deleteStreamData() ->
    # Used to delete stream data
    def deleteStreamData(self, time_stamp_data):

        # Check connection status
        if not self.isConnectionAlive():
            return False

        # Check the time_stamp_data is None or not
        if time_stamp_data:
            # delete a particular data based on the time stamp
            delete_status = self.redisConnection.xdel(self.STREAM_KEY, time_stamp_data)
            if delete_status:
                print(f"Deleted stream data of {time_stamp_data}")
            else:
                print(f"Delete fail for stream data of {time_stamp_data}")
        else:
            # show error
            print("No timestamp data available!!!")
            return False

    # printLogData() ->
    # print latitude, longitude & time
    def printLogData(lat, long, time):
        print(f"Lat  : {lat}")
        print(f"Long : {long}")
        print(f"Time : {time}")
        print("\n")

    # processLocationData() ->
    # Used to process the stream data & generate a list of nearly accurate location data.
    def processLocationData(self, location_data):
        idol_details = dict()
        entry_count = 1

        # print(json.dumps(location_data))
        if(len(location_data) == 0):
            print("No data available!!!")
            return False
        else:
            for loc in location_data[0][1]:
                time_stamp = (str(loc[0])[2:-1])

                if(loc[1][b'idol'] == b'undefined'):
                    print("UNDEFINED VALUE !!! attempt to delete ", time_stamp)
                    if not self.deleteStreamData(time_stamp):
                        print("Delete failed !!!")
                    continue

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
                # self.printLogData(loc[1][b'lat'], loc[1][b'lng'], time)
                entry_count += 1

            print("REPORT : ")
            for idols in idol_details.keys():
                print("Idol : ", idols)
                lat, lng = self.calculateMidPoint(idol_details[idols]['stream_loc'])
                print("Lat  : ",round(lat, 7))
                print("Long : ",round(lng, 7))
                print("\n")
