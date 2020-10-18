from db_connector import DbConnector
from tabulate import tabulate
import pprint
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
        coll_act = self.db["Activity"]
        numUsers = db.User.count()
        numActivities = db.Activity.count()
        averageNumActivites = numActivities/numUsers