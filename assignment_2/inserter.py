from TDT4225.assignment_2.DbConnector import DbConnector

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

    def insert_data(self):
        for user in self.data:
            has_labels = 1 if user in self.labels else 0
            self.insert_user(user, has_labels)
            print("Inserting activities for user with id: {}".format(user))
            for activity in self.data[user]:
                self.insert_activity(user, activity)
                print("\tInserting trackpoints for activity with id: {}".format(activity))
                self.insert_trackpoints(user, activity)

    def insert_user(self, user, has_labels):
        query = "INSERT INTO Users VALUES ('{}',{})".format(user, has_labels)
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_activity(self, user, activity):
        start_date_time = self.data[user][activity][0]
        end_date_time = self.data[user][activity][1]
        transportation_mode = self.data[user][activity][2]
        query = "INSERT INTO Activities VALUES ('{}','{}',{},'{}','{}')"\
            .format(activity, user, transportation_mode, start_date_time, end_date_time)
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_trackpoints(self, user, activity):
        trackpoints = self.data[user][activity][3:]
        for trackpoint in trackpoints:
            lat, lon, altitude = trackpoint[0], trackpoint[1], trackpoint[2]
            date_days, date_time = trackpoint[3], trackpoint[4]
            query = """
                    INSERT INTO Trackpoints (activity_id, lat, lon, altitude, date_days, date_time)
                    VALUES ('{}', '{}', '{}', '{}', '{}', '{}')
                    """.format(activity, lat, lon, altitude, date_days, date_time)
            self.cursor.execute(query)
            self.db_connection.commit()

