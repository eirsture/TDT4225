from collections_creator import CollectionsCreator
from fetcher import Fetcher
from inserter import DBInserter


def main():
    program = None
    fetcher = Fetcher()
    data_set, labels = fetcher.fetch_data(3)
    try:
        #program = CollectionsCreator()
        #program.create_all_collections()
        #program.show_coll()
        program = DBInserter(data_set, labels)
        program.insert_data()
        program.fetch_documents("User")
        program.fetch_documents("Activity")


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

