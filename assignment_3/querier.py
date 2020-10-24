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

    def q6a(self):
        print("6 a)")
        coll_act = self.db["Activity"]

        pipeline = [
            {"$group": {"_id": {"$year": "$start_date_time"}, "NumberOfActivities": {"$sum": 1}}},
            {"$sort": {"NumberOfActivities": -1}},
            {"$limit": 1}
        ]
        documents = coll_act.aggregate(pipeline)

        for doc in documents:
            pprint(doc)

    def q6b(self):
        print("6 b)")
        coll_act = self.db["Activity"]

        pipeline = [
            {"$addFields": {
                "diff_hours": {"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600000]}}},
            {"$group": {"_id": {"$year": "$start_date_time"}, "recorded_hours": {"$sum": "$diff_hours"}}},
            {"$sort": {"recorded_hours": -1}},
            {"$limit": 1}
        ]

        documents = coll_act.aggregate(pipeline)

        for doc in documents:
            pprint(doc)

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
            for i in range(len(trackpoints) - 1):
                a = (trackpoints[i]["lat"], trackpoints[i]["lon"])
                b = (trackpoints[i + 1]["lat"], trackpoints[i + 1]["lon"])
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
