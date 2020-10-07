from dbConnector import DbConnector
from tabulate import tabulate


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

    # Task 10
    # Slow
    def getUsersInForbiddenCity(self):
        query = "SELECT DISTINCT Users.id FROM Users INNER JOIN Activities ON Users.id=Activities.user_id INNER JOIN Trackpoints ON Activities.id=Trackpoints.activity_id WHERE ROUND(Trackpoints.lat, 3)=39.916 AND ROUND(Trackpoints.lon, 3)=116.397"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        print("10) Users who have been in the forbidden city")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    # def execute_delete_query(self, table, query):
    #     self.cursor.execute(query)
    #     self.db_connection.commit()
    #     print("Deleted data from table {}".format(table))

    # def drop_table(self, table_name):
    #     print("Dropping table %s..." % table_name)
    #     query = "DROP TABLE %s"
    #     self.cursor.execute(query % table_name)
    #     self.db_connection.commit()
