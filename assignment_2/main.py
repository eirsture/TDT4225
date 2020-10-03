from tableCreator import DBTablesCreator
from fetcher import Fetcher
from inserter import DBInserter
from querier import DBQuerier


def main():
    fetcher = Fetcher()
    data_set, labels = fetcher.fetch_data()
    try:
        table_creator = DBTablesCreator()
        table_creator.drop_all_tables()
        table_creator.create_all_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        table_creator.connection.close_connection()

    try:
        data_inserter = DBInserter(data_set, labels)
        data_inserter.insert_data()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        data_inserter.connection.close_connection()

    try:
        data_querier = DBQuerier()
        select_users = "SELECT * FROM Users"
        # select_some_activities = "SELECT * FROM Activities WHERE transportation_mode IS NOT NULL"
        # select_some_trackpoints = "SELECT * FROM Trackpoints WHERE id < 20"
        data_querier.execute_display_query('Users', select_users)
        # data_querier.execute_display_query('Activities', select_some_activities)
        # data_querier.execute_display_query('Trackpoints', select_some_trackpoints)
        # delete_035 = "DELETE FROM Users WHERE id='035'"
        # data_querier.execute_delete_query('Users', delete_035)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        data_querier.connection.close_connection()


if __name__ == '__main__':
    main()
