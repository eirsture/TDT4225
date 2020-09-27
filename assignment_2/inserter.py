from DbConnector import DbConnector




class DBInserter:

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
                CREATE TABLE IF NOT EXISTS 'User' (
                id CHAR(3) NOT NULL,
                has_labels BIT(1) NOT NULL,
                PRIMARY KEY id)
                """
        # BIT is commonly used as a boolean operator in MYSQL queries
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """
                CREATE TABLE IF NOT EXISTS 'Activity' (
                id INT AUTO_INCREMENT NOT NULL,
                user_id CHAR(3) NOT NULL,
                transportation_mode VARCHAR(20) NOT NULL,
                start_date_time DATETIME NOT NULL,
                end_date_time DATETIME NOT NULL,
                PRIMARY KEY id,
                CONSTRAINT fk_user FOREIGN KEY user_id
                    REFERENCES 'User' id ON DELETE CASCADE)
                """
        # BIT is commonly used as a boolean operator in MYSQL queries
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        pass

    def create_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   name VARCHAR(30))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()
