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
        print("Data from table {}, tabulated:".format(table))
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
