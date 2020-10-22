from db_connector import DbConnector
from tabulate import tabulate
from pprint import pprint
from haversine import haversine

class DBQuerier:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)

    def q1(self):
        print("1)")

        coll_user = self.db["User"]
        print("Number of users: ", coll_user.count())

        coll_act = self.db["Activity"]
        print("Number of activities: ", coll_act.count())

        coll_tp = self.db["Trackpoint"]
        print("Number of trackpoints: ", coll_tp.count())

    def q2(self):
        print("2)")
        coll_user = self.db["User"]

        coll_act = self.db["Activity"]
        
        numUsers = coll_user.count()
        numActivities = coll_act.count()
        averageNumActivites = numActivities/numUsers
        print("Average number of activities pr user: ", averageNumActivites)

    def q3(self): 
        print("3)")
        coll_act = self.db["Activity"]

        pipeline = [
            { "$group": { "_id": "$user", "NumberOfActivities": { "$sum": 1 }}},
		    { "$sort": { "NumberOfActivities": -1 } },
            { "$limit": 20 }
        ]
        documents = coll_act.aggregate(pipeline)

        for doc in documents: 
            pprint(doc)

    def q6a(self):
        print("6 a)")
        coll_act = self.db["Activity"]

        pipeline = [
            { "$group": { "_id": {"$year": "$start_date_time"}, "NumberOfActivities": { "$sum": 1 }}},
		    { "$sort": { "NumberOfActivities": -1 } },
            { "$limit": 1 }
        ]
        documents = coll_act.aggregate(pipeline)

        for doc in documents: 
            pprint(doc)

    def q6b(self):
        print("6 b)")
        coll_act = self.db["Activity"]

        pipeline = [
            {"$addFields": { "diff_hours": {"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600000]}}},
            { "$group": { "_id": {"$year": "$start_date_time"}, "recorded_hours": { "$sum": "$diff_hours" }}},
		    { "$sort": { "recorded_hours": -1 } },
            { "$limit": 1 }
        ]

        documents = coll_act.aggregate(pipeline)

        for doc in documents: 
            pprint(doc)