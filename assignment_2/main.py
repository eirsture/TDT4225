from tableCreator import DBTablesCreator
from fetcher import Fetcher
from inserter import DBInserter
from querier import DBQuerier


def main():

    try:
        data_querier = DBQuerier()
        # select_users = "SELECT * FROM Users LIMIT 10"
        # select_activities = "SELECT * FROM Activities LIMIT 10"
        # select_trackpoints = "SELECT * FROM Trackpoints LIMIT 10"
        # select_some_activities = "SELECT * FROM Activities WHERE transportation_mode IS NOT NULL"
        # select_some_trackpoints = "SELECT * FROM Trackpoints WHERE id < 20"
        # data_querier.execute_display_query('Users', select_users)
        # data_querier.execute_display_query('Activities', select_activities)
        # data_querier.execute_display_query('Trackpoints', select_trackpoints)
        # delete_035 = "DELETE FROM Users WHERE id='035'"
        # data_querier.execute_delete_query('Users', delete_035)

        # Task 1
        # print("1) Number of users: ", data_querier.countRows("Users"))
        # print("1) Number of activities: ", data_querier.countRows("Activities"))
        # print("1) Number of trackpoints: ", data_querier.countRows("Trackpoints"))

        # Task 2
        # data_querier.getAverageActivitiesPrUser()

        # Task 3
        # data_querier.getTop20ActiveUsers()

        # Task 4
        # data_querier.getUsersTakingTaxi()

        # Task 10
        # data_querier.getUsersInForbiddenCity()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        data_querier.connection.close_connection()


if __name__ == '__main__':
    main()

    """ Can be inserted to main if you want to tear down and build up the database again
        especially be careful with dropping the tables"""

    # fetcher = Fetcher()
    # data_set, labels = fetcher.fetch_data()
    # try:
    #     table_creator = DBTablesCreator()
    #     table_creator.drop_all_tables()
    #     table_creator.create_all_tables()
    #
    # except Exception as e:
    #     print("ERROR: Failed to use database:", e)
    # finally:
    #     table_creator.connection.close_connection()

    # try:
    #     data_inserter = DBInserter(data_set, labels)
    #     data_inserter.insert_data()
    #
    # except Exception as e:
    #     print("ERROR: Failed to use database:", e)
    # finally:
    #     data_inserter.connection.close_connection()
