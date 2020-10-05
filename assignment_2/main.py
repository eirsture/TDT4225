from tableCreator import DBTablesCreator
from fetcher import Fetcher
from inserter import DBInserter
from querier import DBQuerier


def main():

    try:
        data_querier = DBQuerier()
        data_querier.find_invalid_activities()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        data_querier.connection.close_connection()


if __name__ == '__main__':
    main()

    """ The following below can be inserted to main if you want to tear down and build up the database again
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
