from TDT4225.assignment_2.DbConnector import DbConnector
from TDT4225.assignment_2.fetcher import Fetcher
from tabulate import tabulate


class DBTables:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
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
                        transportation_mode VARCHAR(20),
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
                        20081023025304: (activity_id, "plt file name")
                            [[trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                        20081024020959:
                            [[trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                        ...
                    001: (user_id)
                        ...
                    ...
                    181: (user_id)
                        ...
                }

    """

    def __init__(self, data):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.data = data

    def insert_data(self):
        for user in self.data:
            self.insert_user(user)

    def insert_user(self, user):
        has_labels = 1 if 'labels' in self.data[user] else 0
        query = "INSERT INTO Users VALUES ('{}',{})".format(user, has_labels)
        self.cursor.execute(query)
        self.db_connection.commit()

    def fetch_users(self):
        query = "SELECT * FROM Users"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print("Data from table Users, tabulated:")
        print(tabulate(rows, headers=self.cursor.column_names))



def main():
    fetcher = Fetcher()
    data_set = fetcher.fetch_data(12)  # Fetches data from 12 users
    program = None
    try:
        table_creator = DBTables()
        table_creator.drop_all_tables()
        table_creator.create_tables()

        data_inserter = DBInserter(data_set)
        data_inserter.insert_data()
        data_inserter.fetch_users()



    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
