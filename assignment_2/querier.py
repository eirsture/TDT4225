from dbConnector import DbConnector
from tabulate import tabulate
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
        print("Data from table {}, tabulated:".format(table))
        print(tabulate(rows, headers=self.cursor.column_names))


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


    def top_gained_altitude(self):
        # Find the top 20 users who have gained the most altitude meters

        query =  "SELECT Activities.user_id, SUM(To_points.altitude - From_points.altitude) / 3.2808 AS Altitude_gained FROM Trackpoints AS From_points INNER JOIN Trackpoints AS To_points ON From_points.id = To_points.id - 1 AND From_points.activity_id = To_points.activity_id AND To_points.altitude > From_points.altitude INNER JOIN Activities ON From_points.activity_id = Activities.id GROUP BY Activities.user_id ORDER BY Altitude_gained DESC LIMIT 20"
                
        self.execute_display_query("Activities", query)
        

    # def execute_delete_query(self, table, query):
    #     self.cursor.execute(query)
    #     self.db_connection.commit()
    #     print("Deleted data from table {}".format(table))

    # def drop_table(self, table_name):
    #     print("Dropping table %s..." % table_name)
    #     query = "DROP TABLE %s"
    #     self.cursor.execute(query % table_name)
    #     self.db_connection.commit()