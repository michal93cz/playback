import json
import pandas as pd
from pymongo import MongoClient
# from pyspark import SQLContext
# from pyspark.shell import sc

client = MongoClient()
db = client['Playback']
songs_collection = db['songs']


# myrdd = sc.textFile("./data/unique_tracks.txt").map(lambda x: x.split('<SEP>'))


# rawData = spark.read.csv("./data/unique_tracks.txt")
# rawData = spark.read.format("csv").load("./data/unique_tracks.txt")
# rawData.map(lambda x: x.split('<SEP>'))
# print(myrdd.collect())

colnames = ['variation', 'songId', 'title', 'artist']
df = pd.read_csv('./data/unique_tracks.txt', engine='python', sep='<SEP>', names=colnames, header=None)
df = df.drop(columns=['variation'])
df = df.drop_duplicates(subset=['songId'])

records = json.loads(df.T.to_json()).values()
songs_collection.insert(records)
