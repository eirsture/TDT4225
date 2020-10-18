from collections_creator import CollectionsCreator
from fetcher import Fetcher
from inserter import DBInserter


def main():
    """
    # The following below can be inserted to main if you want to tear down and build up the database again
    creator = None
    program = None
    fetcher = Fetcher()
    data_set, labels = fetcher.fetch_data()
    try:
        creator = CollectionsCreator()
        creator.drop_all_collections()
        creator.create_all_collections()

        creator.show_collections()
        program = DBInserter(data_set, labels)
        program.insert_data()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if creator:
            creator.connection.close_connection()
        if program:
            program.connection.close_connection()
    """

if __name__ == '__main__':
    main()

