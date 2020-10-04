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

    # def execute_delete_query(self, table, query):
    #     self.cursor.execute(query)
    #     self.db_connection.commit()
    #     print("Deleted data from table {}".format(table))

    # def drop_table(self, table_name):
    #     print("Dropping table %s..." % table_name)
    #     query = "DROP TABLE %s"
    #     self.cursor.execute(query % table_name)
    #     self.db_connection.commit()
