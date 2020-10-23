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

    def part1(self):
        print("First 10 users:")
        coll_user = self.db["User"]
        usr = coll_user.find({}).limit(10)
        print(tabulate(usr, headers="keys"))
        print("\n\n")

        print("First 10 activities:")
        coll_act = self.db["Activity"]
        act = coll_act.find({}).limit(10)
        print(tabulate(act, headers="keys"))
        print("\n\n")

        print("First 10 trackpoints:")
        coll_tp = self.db["Trackpoint"]
        tp = coll_tp.find({}).limit(10)
        print(tabulate(tp, headers="keys"))

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


