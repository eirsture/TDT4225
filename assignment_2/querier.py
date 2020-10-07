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

    def task5(self):

        query = "SELECT transportation_mode, COUNT(transportation_mode) as number_of FROM Activities WHERE transportation_mode IS NOT NULL GROUP BY(transportation_mode);"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("5) Transportation mode and number of activities of that transportation mode")
        print(tabulate(rows, headers=self.cursor.column_names))

    def task6(self):
        """ Didn't get MAX() to work so did as suggested by: https://stackoverflow.com/a/16133099 """
        "SELECT YEAR(start_date_time) as year, COUNT(*) as number_of FROM Activities GROUP BY YEAR(start_date_time) ORDER BY number_of DESC LIMIT 1"
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



    def task11(self):
        query = "SELECT id FROM Users WHERE has_labels=TRUE"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("6 a) The year with the most activities and number of")
        print(tabulate(rows, headers=self.cursor.column_names))

    # def execute_delete_query(self, table, query):
    #     self.cursor.execute(query)
    #     self.db_connection.commit()
    #     print("Deleted data from table {}".format(table))

    # def drop_table(self, table_name):
    #     print("Dropping table %s..." % table_name)
    #     query = "DROP TABLE %s"
    #     self.cursor.execute(query % table_name)
    #     self.db_connection.commit()
