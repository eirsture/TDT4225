from dbConnector import DbConnector
from tabulate import tabulate
import pprint
from haversine import haversine

class DBQuerier:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def execute_display_query(self, table, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # Using tabulate to show the table in a nice way
        print("Data from table {}:".format(table))
        print()
        print(tabulate(rows, headers=self.cursor.column_names))

    # Task 1
    def countRows(self, table_name):
        query = "SELECT COUNT(id) FROM %s"
        self.cursor.execute(query % table_name)
        results = self.cursor.fetchall()
        return results[0][0]

    # Task 2
    def getAverageActivitiesPrUser(self):
        numUsers = self.countRows("Users")
        numActivities = self.countRows("Activities")
        averageNumActivites = numActivities/numUsers
        print("2) Average number of activites pr user: ", averageNumActivites)
        return averageNumActivites

    # Task 3
    def getTop20ActiveUsers(self):
        query = "SELECT Users.id, COUNT(Users.id) AS NumberOfActivities FROM Activities INNER JOIN Users ON Activities.user_id=Users.id GROUP BY(Users.id) ORDER BY COUNT(Users.id) DESC LIMIT 20"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print("3) Top 20 users with highest number of activities:")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    # Task 4
    def getUsersTakingTaxi(self):
        query = "SELECT DISTINCT Users.id FROM Activities INNER JOIN Users ON Activities.user_id=Users.id WHERE Activities.transportation_mode='taxi'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print("4) Users who have taken taxi")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows
    
    # Task 5
    def task5(self):

        query = "SELECT transportation_mode, COUNT(transportation_mode) as number_of FROM Activities WHERE transportation_mode IS NOT NULL GROUP BY(transportation_mode);"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("5) Transportation mode and number of activities of that transportation mode")
        print(tabulate(rows, headers=self.cursor.column_names))
    
    # Task 6
    def task6(self):
        """ Didn't get MAX() to work so did as suggested by: https://stackoverflow.com/a/16133099 """
        query_a = "SELECT YEAR(start_date_time) as year, COUNT(*) as number_of FROM Activities GROUP BY YEAR(start_date_time) ORDER BY number_of DESC LIMIT 1"
        self.cursor.execute(query_a)
        rows_a = self.cursor.fetchall()
        print("6 a) The year with the most activities")
        print(tabulate(rows_a, headers=self.cursor.column_names), "\n\n")
        
        query_b = "Select YEAR(start_date_time) as year, SUM(TIMEDIFF(end_date_time, start_date_time))/3600 as recorded_hours FROM Activities GROUP BY year ORDER BY recorded_hours DESC LIMIT 1"
        self.cursor.execute(query_b)
        rows_b = self.cursor.fetchall()
        print("6 b) The year with the most recorded hours")
        print(tabulate(rows_b, headers=self.cursor.column_names), "\n\n")
        if (rows_a[0][0] == rows_b[0][0]):
            print("As you can see was " + str(rows_a[0][0]) + " both the year with most activities and the most recorded hours.")
        else:
            print("As you can see was " + str(rows_a[0][0]) + " the year with most activities, while " + str(rows_b[0][0]) + " was the year with the most recorded hours.")
  
    # Task 7
    def total_distance_walked(self):
        # Find the total distance (in km) walked in 2008, by user with id=112
        
        points = []

        
        act_id = "SELECT id FROM Activities WHERE user_id=112"

        self.cursor.execute(act_id)
        rows = self.cursor.fetchall()

        for row in rows:
            trc = "SELECT lat, lon FROM Trackpoints WHERE activity_id =" + str(row[0])

            self.cursor.execute(trc)
            trc_rows = self.cursor.fetchall()

            km = 0

            for i in range(len(trc_rows)-1):
                a = trc_rows[i]
                b = trc_rows[i+1]
                km += haversine(a,b)

            points.append(km)
        
        sum_points = sum(points)
        print("Distance walked by user_id=112: ", sum_points)

    # Task 8
    def top_gained_altitude(self):
        # Find the top 20 users who have gained the most altitude meters

        query =  "SELECT Activities.user_id, SUM(To_points.altitude - From_points.altitude) / 3.2808 AS Altitude_gained FROM Trackpoints AS From_points INNER JOIN Trackpoints AS To_points ON From_points.id = To_points.id - 1 AND From_points.activity_id = To_points.activity_id AND To_points.altitude > From_points.altitude INNER JOIN Activities ON From_points.activity_id = Activities.id GROUP BY Activities.user_id ORDER BY Altitude_gained DESC LIMIT 20"
                
        self.execute_display_query("Activities", query)
        
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
    
    # Task 10
    # Slow
    def getUsersInForbiddenCity(self):
        query = "SELECT DISTINCT Users.id FROM Users INNER JOIN Activities ON Users.id=Activities.user_id INNER JOIN Trackpoints ON Activities.id=Trackpoints.activity_id WHERE ROUND(Trackpoints.lat, 3)=39.916 AND ROUND(Trackpoints.lon, 3)=116.397"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print("10) Users who have been in the forbidden city")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

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
