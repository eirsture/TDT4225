from TDT4225.assignment_2.DbConnector import DbConnector


class DBTablesCreator:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    # Main method
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
        print("Created table Users")

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
        print("Created table Activities")

    def create_trackpoint_table(self):
        query = """
                CREATE TABLE IF NOT EXISTS Trackpoints
                    (
                        id INT AUTO_INCREMENT NOT NULL,
                        activity_id INT NOT NULL,
                        lat DOUBLE NOT NULL,
                        lon DOUBLE NOT NULL,
                        altitude INT,
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
        print("Created table Trackpoints")

    def drop_all_tables(self):
        tables = ['Trackpoints', 'Activities', 'Users']
        for table in tables:
            query = "DROP TABLE %s"
            self.cursor.execute(query % table)
            self.db_connection.commit()
            print("Dropped table %s" % table)
