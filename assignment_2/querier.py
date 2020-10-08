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

        """
        11. Find all users who have registered transportation_mode and their most used transportation_mode. 
            ○ The answer should be on format(user_id, most_used_transportation_mode) sorted on user_id. 
            ○ Some users may have the same number of activities tagged with e.g.walk and car.
                In this case it is up to you to decide which transportation mode to include in your answer(choose one). 
            ○ Do not count the rows where the mode is null.
        """

    def find_most_used_transportation(self):
        query = """
                SELECT 
                    a.user_id AS uid,
                    a.transportation_mode AS t_mode,
                    COUNT(a.transportation_mode) AS counted        
                FROM 
                    Activities a
                INNER JOIN Users u
                    ON a.user_id = u.id AND u.has_labels = 1 AND a.transportation_mode IS NOT NULL 
                GROUP BY uid, t_mode
                ORDER BY uid, counted DESC 
                """

        self.cursor.execute(query)
        user_transportation_data = self.cursor.fetchall()

        data = {}
        for user_data in user_transportation_data:
            user_id = user_data[0]
            transportation_mode = user_data[1]
            if user_id not in data:
                data[user_id] = transportation_mode

        data_pretty = list(data.items())
        print("\n11) All users who have registered transportation_mode and their most used transportation_mode.")
        print(tabulate(data_pretty, headers=("user id", "most used transportation")))


