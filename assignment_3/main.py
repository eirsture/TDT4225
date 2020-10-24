from collections_creator import CollectionsCreator
from fetcher import Fetcher
from inserter import DBInserter
from querier import DBQuerier


def main():
    program = None

    try:
        program = DBQuerier()
        # program.part1()
        # program.q1()
        # program.q2()
        # program.q3()
        # program.q4()
        # program.q5()
        # program.q6a()
        # program.q6b()
        # program.q7()
        # program.q8()
        # program.q9()
        # program.q10()
        # program.q11()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()
    

if __name__ == '__main__':
    main()


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
    data_set, labels = fetcher.fetch_data(3)
    print(data_set)
    print(labels)
    

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if creator:
            creator.connection.close_connection()
        if program:
            program.connection.close_connection()
    """