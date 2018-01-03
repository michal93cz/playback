import json
from bson.json_util import loads
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import time
from dateutil.relativedelta import relativedelta as rd
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

# TIME
start = time.time()

colnames = ['variation', '_id', 'artist', 'title']
df_songs = pd.read_csv('./data/unique_tracks.txt', engine='python', sep='<SEP>', names=colnames, header=None)
df_songs = df_songs.drop(columns=['variation'])
df_songs = df_songs.drop_duplicates(subset=['_id'])


# ARTISTS
df_artists = df_songs['artist']
df_artists = df_artists.drop_duplicates()
records = json.loads(df_artists.T.to_json()).items()
records = list(map((lambda x: {'_id': x[0], 'name': x[1]}), records))
df_artists = pd.DataFrame.from_records(records)
artists_collection.insert(records, {'ordered': False, 'writeConcern': {'w': 0, 'j': False, 'wtimeout': 0}})



print('playback read start')
colnames = ['userId', 'songId', 'dateId']
df_playback = pd.read_csv('./data/triplets_sample_20p.txt', engine='python', sep='<SEP>', names=colnames, header=None
                          , nrows=10000000)


# MERGE SONGS WITH ARTISTS
df_songs = pd.merge(df_songs, df_artists, left_on='artist', right_on='name')
df_songs.drop('artist', axis=1, inplace=True)
df_songs.drop('name', axis=1, inplace=True)
df_songs.columns = ['_id', 'title', 'artist_id']


# MERGE PLAYBACKS WITH SONGS
df_playback = pd.merge(df_playback, df_songs, left_on='songId', right_on='_id')
df_playback.drop('_id', axis=1, inplace=True)
df_playback.drop('title', axis=1, inplace=True)
df_playback.columns = ['userId', 'songId', 'dateId', 'artistId']


# PLAYBACKS
print('playback list start')
records = loads(df_playback.T.to_json()).items()
records = list(map((lambda x: {'_id': x[0], 'userId': x[1]['userId'], 'songId': x[1]['songId'],
                               'dateId': x[1]['dateId'], 'artistId': x[1]['artistId']}), records))
print('playback insert start')
playbacks_collection.insert(records, {'ordered': False, 'writeConcern': {'w': 0, 'j': False, 'wtimeout': 0}})
print('playback inserted')


# SONGS
df_songs.drop('artist_id', axis=1, inplace=True)
records = json.loads(df_songs.T.to_json()).values()
print('songs insert start')
songs_collection.insert(records, {'ordered': False, 'writeConcern': {'w': 0, 'j': False, 'wtimeout': 0}})
print('songs inserted')


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

print('dates insert start')
dates_collection.insert(records, {'ordered': False, 'writeConcern': {'w': 0, 'j': False, 'wtimeout': 0}})
print('dates inserted')


# USERS
df_users = df_playback['userId']
df_users = df_users.drop_duplicates()
records = json.loads(df_dates.T.to_json()).values()
records = list(map((lambda x: {'_id': x}), records))

print('users insert start')
users_collection.insert(records, {'ordered': False, 'writeConcern': {'w': 0, 'j': False, 'wtimeout': 0}})

# TIME
time = time.time() - start
fmt = '{0.days} days {0.hours} hours {0.minutes} minutes {0.seconds} seconds'
print(fmt.format(rd(seconds=time)))
