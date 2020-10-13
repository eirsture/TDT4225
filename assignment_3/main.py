from collections_creator import CollectionsCreator


def main():
    program = None
    try:
        program = CollectionsCreator()
        #program.create_all_collections()
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

