from db_connector import DbConnector
from tabulate import tabulate
from pprint import pprint
from haversine import haversine
from datetime import datetime


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

    # 9. Find all users who have invalid activities, and the number of invalid activities per user
    # ○ An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with
    #   at least 5 minutes.

    def q9(self):
        coll_tp = self.db["Trackpoint"]
        print("Querying all trackpoints (may take a while)...")
        trackpoints = list(coll_tp.find())
        comparing_trackpoints = trackpoints[1:]
        print("Finding time stamp deviations for trackpoints...")
        time_diff_trackpoints = list(map(
            lambda x, y: (x['activity_id'],
                          (x['date_time'] - y['date_time'] if x['activity_id'] == y['activity_id']
                           else x['date_time'] - x['date_time'])  # first trackpoint time diff for an activity set to 0
                          .total_seconds()), comparing_trackpoints, trackpoints))

        invalid_trackpoints = list(filter(lambda x: x[1] >= 300, time_diff_trackpoints))
        print("Finding invalid activities with trackpoint time stamp deviations > 5 minutes...")
        invalid_activities = set()
        for trackpoint in invalid_trackpoints:
            activity_id = trackpoint[0]
            invalid_activities.add(activity_id)

        print("Finding users associated with the invalid activities...")
        users = {}
        coll_act = self.db["Activity"]
        for activity_id in invalid_activities:
            activity = coll_act.find_one({'_id': activity_id})
            user = activity['user']
            if user not in users:
                users[user] = 1
            else:
                users[user] += 1

        users_pretty = list(users.items())
        print("\n9)All users who have invalid activities, and the number of invalid activities per user")
        print(tabulate(users_pretty, headers=("user id", "invalid activities")))

    """
        11. Find all users who have registered transportation_mode and their most used transportation_mode. 
            ○ The answer should be on format(user_id, most_used_transportation_mode) sorted on user_id. 
            ○ Some users may have the same number of activities tagged with e.g.walk and car.
                In this case it is up to you to decide which transportation mode to include in your answer(choose one). 
            ○ Do not count the rows where the mode is null.
    """

    def q11(self):
        coll_act = self.db["Activity"]
        query = {"transportation_mode": {"$ne": None}}
        labeled_activities = list(coll_act.find(query))

        data = {}
        for activity in labeled_activities:
            user_id = activity['user']
            transportation_mode = activity['transportation_mode']
            if user_id not in data:
                data[user_id] = {}
            if transportation_mode not in data[user_id]:
                data[user_id][transportation_mode] = 1
            else:
                data[user_id][transportation_mode] += 1

        data_pretty = []
        for user in data:
            most_used_transportation_mode = None
            max_times_used = -1
            for transportation_mode in data[user]:
                times_used = data[user][transportation_mode]
                if times_used > max_times_used:
                    max_times_used = times_used
                    most_used_transportation_mode = transportation_mode
            data_pretty.append((user, most_used_transportation_mode))

        print("\n11) All users who have registered transportation_mode and their most used transportation_mode.")
        print(tabulate(data_pretty, headers=("user id", "most used transportation")))
