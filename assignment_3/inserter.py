from db_connector import DbConnector
from datetime import datetime
from pprint import pprint

"""
    Data structure:
        self.data:
            {
                000: (user_id)
                    1: (activity_id, "plt file name")
                        [<activity start datetime>, <activity end datetime>, <transportation_mode>
                            [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                    2:
                        [<activity start datetime>, <activity end datetime>, <transportation_mode> 
                            [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                    ...
                001: (user_id)
                    ...
                ...
                181: (user_id)
                    ...
            }
            
        trackpoint example:
            ['40.127951', '116.48646', '62', '39969.065474537', '2009-06-05 01:34:17']



        self.labels: (see txt file labeled_ids for more info)
            {
                010:
                    [[label 1], [label 2], ..., [label N]]
                020:
                    [[label 1], [label 2], ..., [label N]]
                ...
                179:
                    [[label 1], [label 2], ..., [label N]]
            }
            
        label example:
            ['2007-06-26 11:32:29', '2007-06-26 11:40:29', 'bus']
"""


class DBInserter:

    # Takes in dictionary for all user/activity/trackpoint data and label dictionary linked to users
    def __init__(self, data, labels):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.data = data
        self.labels = labels
        self.users = []
        self.activities = []
        self.trackpoints = []
        self.trackpoint_id = 0

    def prepare_data(self):

        for user in self.data:
            print("Preparing for user: ", user)
            has_labels = True if user in self.labels else False
            self.users.append({"_id": int(user), "has_labels": has_labels})

            for activity in self.data[user]:
                self.prepare_activities(user, activity)
                self.prepare_trackpoints(user, activity)

    def prepare_activities(self, user, activity):
        start_date_time = self.data[user][activity][0]
        end_date_time = self.data[user][activity][1]
        transportation_mode = self.data[user][activity][2]
        self.activities.append({
            "_id": activity,
            "user": int(user),
            "transportation_mode": transportation_mode,
            "start_date_time": datetime.strptime(start_date_time, '%Y-%m-%d %H:%M:%S'),
            "end_date_time": datetime.strptime(end_date_time, '%Y-%m-%d %H:%M:%S')
        })

    def prepare_trackpoints(self, user, activity):
        trackpoints = self.data[user][activity][3:]
        for trackpoint in trackpoints:
            lat, lon, altitude = trackpoint[0], trackpoint[1], trackpoint[2]
            date_days, date_time = trackpoint[3], trackpoint[4]
            self.trackpoints.append({
                "_id": self.trackpoint_id,
                "activity_id": activity,
                "lat": float(lat),
                "lon": float(lon),
                "altitude": float(altitude),
                "date_days": float(date_days),
                "date_time": datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')

            })
            self.trackpoint_id += 1

    def insert_data(self):
        self.prepare_data()
        print("Prepared users count: ", len(self.users))
        print("Prepared activities count: ", len(self.activities))
        print("Prepared trackpoints count: ", len(self.trackpoints))

        self.insert_user()
        self.insert_activity()
        self.insert_trackpoints()

    def insert_user(self):
        print("Inserting users")
        collection = self.db["User"]
        collection.insert_many(self.users)

    def insert_activity(self):
        print("Inserting activities")
        collection = self.db["Activity"]
        collection.insert_many(self.activities)

    def insert_trackpoints(self):
        print("Inserting trackpoints")
        collection = self.db["Trackpoint"]
        collection.insert_many(self.trackpoints)

