from locationStreamManager import locationStreamManager

if __name__ == "__main__":
    lsm = locationStreamManager()

    lsm2 = locationStreamManager(redis_connection = lsm.redisConnection)

    stream_data = lsm2.getAllStreamData()

    if(stream_data):
        lsm2.processLocationData(stream_data)
    else:
        print(f"No data available !!! for the stream key : {lsm2.STREAM_KEY}")
