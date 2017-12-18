import json
import pandas as pd
from pymongo import MongoClient

client = MongoClient()
db = client['Playback']
songs_collection = db['songs']
artists_collection = db['artists']

colnames = ['variation', 'songId', 'artist', 'title']
df = pd.read_csv('./data/unique_tracks.txt', engine='python', sep='<SEP>', names=colnames, header=None)
df = df.drop(columns=['variation'])
df = df.drop_duplicates(subset=['songId'])
records = json.loads(df.T.to_json()).values()
songs_collection.insert(records)

df_artists = df['artist']
df_artists = df_artists.drop_duplicates()
# records = json.loads(df_artists.T.to_json()).values()
# artists_collection.insert(records)
print(df_artists)
