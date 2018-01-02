from pprint import pprint
import time

from pymongo import MongoClient

client = MongoClient()
db = client['Playback']
songs_collection = db['songs']
artists_collection = db['artists']
dates_collection = db['dates']
playbacks_collection = db['playbacks']
users_collection = db['users']

# # ranking piosenek
# start = time.time()
# pprint(list(playbacks_collection.aggregate([
#     {'$group': {'_id': '$songId', 'count': {'$sum': 1}}},
#     {'$sort': {'count': -1}},
#     {'$limit': 15},
#     {
#         '$lookup': {
#             'from': 'songs',
#             'localField': '_id',
#             'foreignField': '_id',
#             'as': 'song'
#         }
#     },
#     {'$match': {'song': {'$ne': []}}}
# ])))
# end = time.time()
# print(end - start)

# ranking użytkowników
# start = time.time()
# pprint(list(playbacks_collection.aggregate([
#     {'$group': {'_id': '$userId', 'count': {'$sum': 1}}},
#     {'$sort': {'count': -1}},
#     {'$limit': 15}
# ])))
# end = time.time()
# print(end - start)

# ranking artystów
# start = time.time()
# pprint(list(playbacks_collection.aggregate([
#     {'$group': {'_id': '$artistId', 'count': {'$sum': 1}}},
#     {'$sort': {'count': -1}},
#     {'$limit': 15},
#     {
#         '$lookup': {
#             'from': 'artists',
#             'localField': '_id',
#             'foreignField': '_id',
#             'as': 'artist'
#         }
#     },
#     {'$match': {'artist': {'$ne': []}}}
# ])))
# end = time.time()
# print(end - start)


# start = time.time()
# pprint(list(playbacks_collection.aggregate([
#     {'$group': {'_id': '$songId', 'count': {'$sum': 1}}},
#     {'$sort': {'count': -1}},
#     {'$limit': 15},
#     {
#         '$lookup': {
#             'from': 'songs',
#             'localField': '_id',
#             'foreignField': '_id',
#             'as': 'song'
#         }
#     },
#     {'$match': {'song': {'$ne': []}}}
# ])))
# end = time.time()
# print(end - start)


# sumaryczna liczba odsłuchań w podziale na poszczególne miesiące
start = time.time()
pprint(list(dates_collection.aggregate([
    {'$group': {'_id': '$month', 'count': {'$sum': 1}}},
    {'$sort': {'_id': 1}}
])))
end = time.time()
print(end - start)