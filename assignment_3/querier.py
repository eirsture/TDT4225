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
        print("1)\na)")
        print("Number of users: ", coll_user.count())

        coll_act = self.db["Activity"]
        print("\nb)\nNumber of activities: ", coll_act.count())

        coll_tp = self.db["Trackpoint"]
        print("\nc)\nNumber of trackpoints: ", coll_tp.count())

    def q2(self):
        coll_user = self.db["User"]

        coll_act = self.db["Activity"]

        numUsers = coll_user.count()
        numActivities = coll_act.count()
        averageNumActivites = numActivities / numUsers
        print("\n2)\nAverage number of activities pr user: ", averageNumActivites)

    def q3(self):
        coll_act = self.db["Activity"]

        pipeline = [
            {"$group": {"_id": "$user", "NumberOfActivities": {"$sum": 1}}},
            {"$sort": {"NumberOfActivities": -1}},
            {"$limit": 20}
        ]
        documents = list(coll_act.aggregate(pipeline))

        print("\n3) Top 20 users with highest number of activities:")
        pprint(documents)

    def q4(self):
        coll_act = self.db["Activity"]

        pipeline = [
            {"$match": {"transportation_mode": {"$eq": "taxi"}}},
            {"$group": {"_id": "$user"}},
            {"$sort": {"_id": 1}},
        ]
        documents = list(coll_act.aggregate(pipeline))

        print("\n4) Users who have taken a taxi:")
        pprint(documents)

    def q9(self):
        coll_tp = self.db["Trackpoint"]
        docs = coll_tp.find()
        for doc in docs:
            pprint(doc)


        # comparing_trackpoints = trackpoints[1:]
        #
        # print("Finding time stamp deviations for trackpoints...")
        # time_diff_trackpoints = list(map(
        #     lambda x, y: (x[0], x[1], (x[2] - y[2]).total_seconds()), comparing_trackpoints, trackpoints))
        #
        # invalid_trackpoints = list(filter(lambda x: x[2] >= 300, time_diff_trackpoints))
        #
        # print("Finding invalid activities with trackpoint time stamp deviations > 5 minutes...")
        # invalid_activities = set()
        # for trackpoint in invalid_trackpoints:
        #     activity_id = trackpoint[1]
        #     invalid_activities.add(activity_id)
        #
        # print("Finding users associated with the invalid activities...")
        # users = {}
        # for activity in invalid_activities:
        #     user_query = "SELECT a.user_id FROM Activities a WHERE a.id = {}".format(activity)
        #     self.cursor.execute(user_query)
        #     user = self.cursor.fetchone()[0]
        #     if user not in users:
        #         users[user] = 1
        #     else:
        #         users[user] += 1
        #
        # users_pretty = list(users.items())
        # print("\n9)All users who have invalid activities, and the number of invalid activities per user")
        # print(tabulate(users_pretty, headers=("user id", "invalid activities")))
