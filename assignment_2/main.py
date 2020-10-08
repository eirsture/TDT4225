from tableCreator import DBTablesCreator
from fetcher import Fetcher
from inserter import DBInserter
from querier import DBQuerier


def main():

    try:
        data_querier = DBQuerier()
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
        
        # Task 5
        # data_querier.task5()
        
        # Task 6
        # data_querier.task6()
        
        # Task 7
        # data_querier.total_distance_walked()
        
        # Task 8
        # data_querier.top_gained_altitude()
        
        # Task 9
        # data_querier.find_invalid_activities()

        # Task 10
        # data_querier.getUsersInForbiddenCity()
        
        # Task 11
        # data_querier.find_most_used_transportation()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        data_querier.connection.close_connection()


if __name__ == '__main__':
    main()

    """ The following below can be inserted to main if you want to tear down and build up the database again """

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
