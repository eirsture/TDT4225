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

        for doc in documents: 
            pprint(doc)

    def q4(self):
        coll_act = self.db["Activity"]

        pipeline = [
            {"$match": {"transportation_mode": {"$eq":"'taxi'"}}},
            {"$group": {"_id":"$user"}},
        ]
        documents = coll_act.aggregate(pipeline)

        for doc in documents:
            pprint(doc)