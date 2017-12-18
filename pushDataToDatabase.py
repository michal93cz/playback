import json
import pandas as pd
from pymongo import MongoClient

client = MongoClient()
db = client['Playback']
songs_collection = db['songs']
artists_collection = db['artists']

colnames = ['variation', '_id', 'artist', 'title']
df_songs = pd.read_csv('./data/unique_tracks.txt', engine='python', sep='<SEP>', names=colnames, header=None)
df_songs = df_songs.drop(columns=['variation'])
df_songs = df_songs.drop_duplicates(subset=['_id'])

df_artists = df_songs['artist']
df_artists = df_artists.drop_duplicates()
records = json.loads(df_artists.T.to_json()).values()
records = list(map((lambda x: {'name': x}), records))
artists_collection.insert(records)

records = json.loads(df_songs.T.to_json()).values()
songs_collection.insert(records)

colnames = ['userId', 'songId', 'time']
df_playback = pd.read_csv('./data/triplets_sample.txt', engine='python', sep='<SEP>', names=colnames, header=None)
print(df_playback)
