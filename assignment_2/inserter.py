from TDT4225.assignment_2.DbConnector import DbConnector
from TDT4225.assignment_2.fetcher import Fetcher
from tabulate import tabulate


class DBTables:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_all_tables(self):
        self.create_user_table()
        self.create_activity_table()
        self.create_trackpoint_table()

    def create_user_table(self):
        query = """
                CREATE TABLE IF NOT EXISTS Users
                    (
                        id CHAR(3) NOT NULL,
                        has_labels BIT(1) NOT NULL,
                        PRIMARY KEY (id)
                    )
                """
        # BIT(1) is commonly used as a boolean operator in MYSQL queries
        self.cursor.execute(query)
        self.db_connection.commit()
        self.show_all_tables()

    def create_activity_table(self):
        query = """
                CREATE TABLE IF NOT EXISTS Activities 
                    (
                        id INT NOT NULL,
                        user_id CHAR(3) NOT NULL,
                        transportation_mode VARCHAR(30),
                        start_date_time DATETIME NOT NULL,
                        end_date_time DATETIME NOT NULL,
                        PRIMARY KEY (id),
                        FOREIGN KEY (user_id) 
                            REFERENCES Users(id) 
                            ON DELETE CASCADE
                            ON UPDATE CASCADE 
                    )
                """
        self.cursor.execute(query)
        self.db_connection.commit()
        self.show_all_tables()

    def create_trackpoint_table(self):
        query = """
                CREATE TABLE IF NOT EXISTS Trackpoints 
                    (
                        id INT AUTO_INCREMENT NOT NULL,
                        activity_id INT NOT NULL,
                        lat DOUBLE,
                        date_days DOUBLE NOT NULL,
                        date_time DATETIME NOT NULL,
                        PRIMARY KEY (id),
                        FOREIGN KEY (activity_id) 
                            REFERENCES Activities(id) 
                            ON DELETE CASCADE
                            ON UPDATE CASCADE 
                    )
                """
        self.cursor.execute(query)
        self.db_connection.commit()
        self.show_all_tables()

    def show_all_tables(self):
        self.cursor.execute("SHOW TABLES")
        tables = self.cursor.fetchall()
        print(tables)

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def drop_all_tables(self):
        tables = ['Trackpoints', 'Activities', 'Users']
        for table in tables:
            query = "DROP TABLE %s"
            self.cursor.execute(query % table)


class DBInserter:
    """
        Data structure:
            self.data:
                {
                    000: (user_id)
                        1: (activity_id, "plt file name")
                            [<activity start datetime>, <activity end datetime>,
                                [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                        2:
                            [<activity start datetime>, <activity end datetime>,
                                [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                        ...
                    001: (user_id)
                        ...
                    ...
                    010: (userid) <this user has a label>
                        'labels': [[label 1], [label 2], ..., [label N]]
                        1239: (activity_id, "plt file name")
                            [<activity start datetime>, <activity end datetime>,
                                [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                        1240:
                            [<activity start datetime>, <activity end datetime>,
                                [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                        ...
                    011: (user_id)
                        1679: (activity_id, "plt file name")
                            [<activity start datetime>, <activity end datetime>,
                                [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                        1680:
                            [<activity start datetime>, <activity end datetime>,
                                [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                        ...
                    ...
                    181: (user_id)
                        ...
                }

            label example:
                ['2007/06/26 11:32:29', '2007/06/26 11:40:29', 'bus']

            trackpoint example:
                ['40.127951', '116.48646', '0', '62', '39969.065474537', '2009-06-05', '01:34:17']

    """

    def __init__(self, data):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.data = data

    def insert_data(self):
        for user in self.data:
            has_labels = 1 if 'labels' in self.data[user] else 0
            self.insert_user(user, has_labels)
            print("Inserting activities for user with id: {}".format(user))
            for activity in self.data[user]:
                self.insert_activity(user, activity, has_labels)


    def insert_user(self, user, has_labels):
        query = "INSERT INTO Users VALUES ('{}',{})".format(user, has_labels)
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_activity(self, user, activity, has_labels):
        transportation_mode = None
        start_date_time = self.data[user][activity][0]
        end_date_time = self.data[user][activity][1]
        if has_labels:
            transportation_mode = self.determine_transportation(user, start_date_time, end_date_time)
        query = "INSERT INTO Activities VALUES ('{}','{}','{}','{}','{}')"\
            .format(activity, user, transportation_mode, start_date_time, end_date_time)
        self.cursor.execute(query)
        self.db_connection.commit()

    def determine_transportation(self, user, start_date_time, end_date_time):
        labeled_activities = self.data[user]['labels']
        for activity in labeled_activities:
            if activity[0] == start_date_time and activity[1] == end_date_time:
                return activity[2]
        return None

    def fetch_users(self):
        query = "SELECT * FROM Users"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # Using tabulate to show the table in a nice way
        print("Data from table Users, tabulated:")
        print(tabulate(rows, headers=self.cursor.column_names))

    def fetch_activities(self):
        query = "SELECT * FROM Activities"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # Using tabulate to show the table in a nice way
        print("Data from table Users, tabulated:")
        print(tabulate(rows, headers=self.cursor.column_names))

    def execute_query(self, table, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # Using tabulate to show the table in a nice way
        print("Data from table {}, tabulated:".format(table))
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    fetcher = Fetcher()
    data_set = fetcher.fetch_data(0)  # Fetches data from approximately 100 users
    try:
        table_creator = DBTables()
        # table_creator.drop_all_tables()
        # table_creator.create_all_tables()

        data_inserter = DBInserter(data_set)
        query = "SELECT * FROM Users"
        data_inserter.execute_query("Users", query)



    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        table_creator.connection.close_connection()
        data_inserter.connection.close_connection()


if __name__ == '__main__':
    main()
