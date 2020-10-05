from dbConnector import DbConnector
from tabulate import tabulate
import pprint



class DBQuerier:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def execute_display_query(self, table, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # Using tabulate to show the table in a nice way
        print("Data from table {}, tabulated:".format(table))
        print(tabulate(rows, headers=self.cursor.column_names))

    # 9. Find all users who have invalid activities, and the number of invalid activities per user
    # ○ An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with
    #   at least 5 minutes.

    def find_invalid_activities(self):
        query = "SELECT t.id, t.activity_id, t.date_time FROM Trackpoints t"
        print('Querying: "{}" ...'.format(query))
        self.cursor.execute(query)
        trackpoints = self.cursor.fetchall()
        comparing_trackpoints = trackpoints[1:]

        print("Finding time stamp deviations for trackpoints...")
        time_diff_trackpoints = list(map(
            lambda x, y: (x[0], x[1], (x[2] - y[2]).total_seconds()), comparing_trackpoints, trackpoints))

        invalid_trackpoints = list(filter(lambda x: x[2] >= 300, time_diff_trackpoints))

        print("Finding invalid activities with trackpoint time stamp deviations > 5 minutes...")
        invalid_activities = set()
        for trackpoint in invalid_trackpoints:
            activity_id = trackpoint[1]
            invalid_activities.add(activity_id)

        print("Finding users associated with the invalid activities...")
        users = {}
        for activity in invalid_activities:
            user_query = "SELECT a.user_id FROM Activities a WHERE a.id = {}".format(activity)
            self.cursor.execute(user_query)
            user = self.cursor.fetchone()[0]
            if user not in users:
                users[user] = 1
            else:
                users[user] += 1

        users_pretty = list(users.items())
        print("\n9)All users who have invalid activities, and the number of invalid activities per user")
        print(tabulate(users_pretty, headers=("user id", "invalid activities")))

