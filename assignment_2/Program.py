import os
from DbConnector import DbConnector

class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        query = """CREATE TABLE IF NOT EXISTS User (
                           id VARCHAR(16) NOT NULL PRIMARY KEY,
                           has_labels BOOLEAN
                           )
                        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """CREATE TABLE IF NOT EXISTS Activity (
                           id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                           user_id VARCHAR(16),
                           transportation_mode VARCHAR(16),
                           start_date_time DATETIME,
                           end_time_date DATETIME,
                           FOREIGN KEY (user_id) REFERENCES User(id) ON UPDATE CASCADE ON DELETE CASCADE
                           )
                        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        query = """CREATE TABLE IF NOT EXISTS Trackpoint (
                           id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                           activity_id INT,
                           lat DOUBLE,
                           lon DOUBLE,
                           altitude INT,
                           date_days DOUBLE,
                           date_time DATETIME,
                           FOREIGN KEY (activity_id) REFERENCES Activity(id) ON UPDATE CASCADE ON DELETE CASCADE
                           )
                        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def read_labeled_ids(self, path):
        with open(path) as f:
            return f.read().splitlines()

    def get_user_ids(self, path):
        return os.listdir(path)




def main():
    program = None
    try:
        program = Program()
        program.create_user_table()
        program.create_activity_table()
        program.create_trackpoint_table()
        print(program.read_labeled_ids('dataset/labeled_ids.txt'))
        print(program.get_user_ids('dataset/Data'))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
