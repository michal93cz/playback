import json
from bson.json_util import loads
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from pprint import pprint

client = MongoClient()
db = client['Playback']

db.drop_collection('songs')
songs_collection = db['songs']

db.drop_collection('artists')
artists_collection = db['artists']

db.drop_collection('dates')
dates_collection = db['dates']

db.drop_collection('playbacks')
playbacks_collection = db['playbacks']

db.drop_collection('users')
users_collection = db['users']


colnames = ['variation', '_id', 'artist', 'title']
df_songs = pd.read_csv('./data/unique_tracks.txt', engine='python', sep='<SEP>', names=colnames, header=None)
df_songs = df_songs.drop(columns=['variation'])
df_songs = df_songs.drop_duplicates(subset=['_id'])


# ARTISTS
df_artists = df_songs['artist']
df_artists = df_artists.drop_duplicates()
records = json.loads(df_artists.T.to_json()).items()
records = list(map((lambda x: {'_id': x[0], 'name': x[1]}), records))
artists_collection.insert(records)


# PLAYBACKS
print('playback read start')
colnames = ['userId', 'songId', 'dateId']
df_playback = pd.read_csv('./data/triplets_sample_20p.txt', engine='python', sep='<SEP>', names=colnames, header=None
                          , nrows=10000)
print('playback list start')
records = list(loads(df_playback.T.to_json()).values())

# TO MUSIMY ZASTĄPIC:
# for index, row in df_playback.iterrows():
#     print(index)
#     song_row = df_songs.loc[df_songs['_id'] == row['songId']]
#     artist_name = song_row.iloc[0]['artist']
#     artist_id = artists_collection.find_one({'name': artist_name})['_id']
#     records[index]['artistId'] = artist_id

print('playback insert start')
playbacks_collection.insert(records)
print('playback inserted')


# SONGS
df_songs.drop('artist', axis=1, inplace=True)
records = json.loads(df_songs.T.to_json()).values()
songs_collection.insert(records)


# DATES
df_dates = df_playback['dateId']
df_dates = df_dates.drop_duplicates()
records = json.loads(df_dates.T.to_json()).values()
records = list(map((lambda x: {
    '_id': x,
    'day': datetime.fromtimestamp(x).day,
    'month': datetime.fromtimestamp(x).month,
    'year': datetime.fromtimestamp(x).year,
    'weekday': datetime.fromtimestamp(x).weekday()
}), records))
dates_collection.insert(records)


# USERS
df_users = df_playback['userId']
df_users = df_users.drop_duplicates()
records = json.loads(df_dates.T.to_json()).values()
records = list(map((lambda x: {'_id': x}), records))
users_collection.insert(records)
