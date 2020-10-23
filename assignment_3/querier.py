from db_connector import DbConnector
from tabulate import tabulate
from pprint import pprint
from haversine import haversine
import time
from datetime import datetime, timedelta


def print_result(documents):
    for doc in documents:
        pprint(doc)


class DBQuerier:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        print_result(documents)

    def q1(self):
        coll_user = self.db["User"]
        print("Number of users: ", coll_user.count())

        coll_act = self.db["Activity"]
        print("Number of activities: ", coll_act.count())

        coll_tp = self.db["Trackpoint"]
        print("Number of trackpoints: ", coll_tp.count())

    def q2(self):
        coll_user = self.db["User"]

        coll_act = self.db["Activity"]
        
        numUsers = coll_user.count()
        numActivities = coll_act.count()
        averageNumActivites = numActivities/numUsers
        print("Average number of activities pr user: ", averageNumActivites)

    def q3(self): 
        coll_act = self.db["Activity"]

        pipeline = [
            { "$group": { "_id": "$user", "NumberOfActivities": { "$sum": 1 }}},
		    { "$sort": { "NumberOfActivities": -1 } },
            { "$limit": 20 }
        ]
        documents = coll_act.aggregate(pipeline)

        print_result(documents)
        
    def q4(self):
        coll_act = self.db["Activity"]

        pipeline = [
            {"$match": {"transportation_mode": {"$eq":"'taxi'"}}},
            {"$group": {"_id":"$user"}},
        ]
        documents = coll_act.aggregate(pipeline)

        print_result(documents)

    def q5(self):
        coll_act = self.db["Activity"]
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        documents = coll_act.aggregate(pipeline)
        print("5)")
        print_result(documents)

    def q7(self):
        start_time = time.time()
        coll_act = self.db["Activity"]
        pipeline = [
            {"$match": {
                "user": 112,
                "transportation_mode": "walk",
                "start_date_time": {"$gte": datetime(year=2008, month=1, day=1)},
                "end_date_time": {"$lte": datetime(year=2008, month=12, day=31)}
            }},
            {"$lookup": {
                "from": "Trackpoint",
                "localField": "_id",
                "foreignField": "activity_id",
                "as": "trackpoints"
            }}
        ]
        documents = coll_act.aggregate(pipeline)
        fetch_time = time.time()
        print(f'Time to fetch from database: {str(timedelta(seconds=(fetch_time - start_time)))}')
        
        km = 0
        for activity in documents:
            trackpoints = activity["trackpoints"]
            for i in range(len(trackpoints)-1):
                a = (trackpoints[i]["lat"], trackpoints[i]["lon"])
                b = (trackpoints[i+1]["lat"], trackpoints[i+1]["lon"])
                km += haversine(a, b)
        print(f'Time to calculate total distance: {str(timedelta(seconds=(time.time() - fetch_time)))}')
        print(f"7) Distance walked by user_id=112 in 2008: {km} km")

    def q8(self):
        coll_tp = self.db["Trackpoint"]

        pipeline=[
            {
            "$match": 
                {'altitude': {'$ne': '-777'}}
                        
            },
            {"$lookup": {
                "from": "Activity",
                "localField": "activity_id",
                "foreignField": "_id",
                "as": "activity"
                }
            },
            {"$project": {
                "_id": 0,
                "altitude": 1,
                "user": "$activity.user"
                }
            }
        ]

        documents = coll_tp.aggregate(pipeline)

        total_altitude_gained = 0
        count = 0
        curr_user = 0

        altitude_list = []

        for doc in documents:
            if(doc["user"][0] != curr_user):
                altitude_list.append({"user": curr_user, "alt": total_altitude_gained})
                curr_user = doc["user"][0]
                total_altitude_gained = 0
                count = 0
            if count > 0:
                if doc["altitude"] > last_doc["altitude"]:
                    total_altitude_gained += doc["altitude"] - last_doc["altitude"]
            count += 1
            last_doc = doc
        altitude_list.append({"user": curr_user, "alt": total_altitude_gained * 0.3048})

        top_20 = sorted(altitude_list, key = lambda i: i["altitude"])[-20:]
        top_20.reverse()
        print("8) Top 20 users who have gained the most altitude meters: ")
        pprint(top_20)


def q10(self): 
    coll_tp = self.db["Trackpoint"]

    pipeline=[ 
        {
        "$match": {
            "$expr": {
                "$and": [
                    {"$eq": [{"$round": ["$lat", 3]}, 39.916]},
                    {"$eq": [{"$round": ["$lon", 3]}, 116.397]}
                    ]
                }
            }
        },
        {"$lookup": {
            "from": "Activity",
            "localField": "activity_id",
            "foreignField": "_id",
            "as": "activity"
            }
        },
        {
            "$unwind": "$activity"
        },
        {
            "$group":
                {
                    "_id": "$activity.user"
                }
        },
    ]

    documents = coll_tp.aggregate(pipeline)

    print("10) Users who have been in the forbidden city: ")
    print_result(documents)