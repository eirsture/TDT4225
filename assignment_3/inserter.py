from dbConnector import DbConnector

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
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.data = data
        self.labels = labels
        self.users = []
        self.activities = []
        self.trackpoints = []

    def prepare_users(self):
        trackpoint_id = 0
        for user in self.data:
            has_labels = 1 if user in self.labels else 0
            self.users.append({"_id": user, "has_labels": has_labels})

            for activity in self.data[user]:
                self.prepare_activities(activity, user)
                trackpoint_id = self.prepare_trackpoints(user, activity, trackpoint_id)

    def prepare_activities(self, user, activity):
        start_date_time = self.data[user][activity][0]
        end_date_time = self.data[user][activity][1]
        transportation_mode = self.data[user][activity][2]
        self.activities.append({
            "_id": activity,
            "user": user,
            "transportation_mode": transportation_mode,
            "start_date_time": start_date_time,
            "end_date_time": end_date_time
        })

    def prepare_trackpoint(self, user, activity, trackpoint_id):
        trackpoints = self.data[user][activity][3:]

        for trackpoint in trackpoints:
            lat, lon, altitude = trackpoint[0], trackpoint[1], trackpoint[2]
            date_days, date_time = trackpoint[3], trackpoint[4]
            self.trackpoints.append({
                "_id": trackpoint_id,
                "activity_id": activity,
                "lat": lat,
                "lon": lon,
                "altitude": altitude,
                "date_days": date_days,
                "date_time": date_time

            })
            trackpoint_id += 1
        return trackpoint_id

    def insert_data(self):
        self.insert_user()
        self.insert_activity()
        self.insert_trackpoints()

    def insert_user(self):
        collection = self.db["User"]
        collection.insert_many(self.users)

    def insert_activity(self):
        collection = self.db["Activity"]
        collection.insert_many(self.activities)

    def insert_trackpoints(self):
        collection = self.db["Trackpoint"]
        collection.insert_many(self.trackpoints)

