from TDT4225.assignment_2.tableCreator import DBTablesCreator
from TDT4225.assignment_2.fetcher import Fetcher
from TDT4225.assignment_2.inserter import DBInserter
from TDT4225.assignment_2.querier import DBQuerier


def main():
    fetcher = Fetcher()
    data_set, labels = fetcher.fetch_data(15)  # Fetches data from approximately 30 users
    data_querier = None
    try:
        table_creator = DBTablesCreator()
        table_creator.drop_all_tables()
        table_creator.create_all_tables()

        data_inserter = DBInserter(data_set, labels)
        data_inserter.insert_data()

        data_querier = DBQuerier()
        select_users = "SELECT * FROM Users"
        select_some_activities = "SELECT * FROM Activities WHERE id < 20"
        data_querier.execute_display_query('Users', select_users)
        data_querier.execute_display_query('Activities', select_some_activities)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if data_querier:
            data_querier.connection.close_connection()
        table_creator.connection.close_connection()
        data_inserter.connection.close_connection()


if __name__ == '__main__':
    main()