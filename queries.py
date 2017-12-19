from pprint import pprint

from pymongo import MongoClient

client = MongoClient()
db = client['Playback']
songs_collection = db['songs']
artists_collection = db['artists']
dates_collection = db['dates']
playbacks_collection = db['playbacks']
users_collection = db['users']

# ranking piosenek
pprint(list(playbacks_collection.aggregate([
    {'$group': {'_id': '$songId', 'count': {'$sum': 1}}},
    {'$sort': {'count': -1}},
    {'$limit': 10},
    {
        '$lookup': {
            'from': 'songs',
            'localField': '_id',
            'foreignField': '_id',
            'as': 'song'
        }
    },
    {'$match': {'song': {'$ne': []}}}
])))

# ranking użytkowników
pprint(list(playbacks_collection.aggregate([
    {'$group': {'_id': '$userId', 'count': {'$sum': 1}}},
    {'$sort': {'count': -1}},
    {'$limit': 10}
])))

# ranking artystów
pprint(list(playbacks_collection.aggregate([
    {'$group': {'_id': '$artistId', 'count': {'$sum': 1}}},
    {'$sort': {'count': -1}},
    {'$limit': 10},
    {
        '$lookup': {
            'from': 'artists',
            'localField': '_id',
            'foreignField': '_id',
            'as': 'artist'
        }
    },
    {'$match': {'artist': {'$ne': []}}}
])))
