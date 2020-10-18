from db_connector import DbConnector
from decouple import config

class CollectionsCreator:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.collection_names = ['User', 'Activity', 'Trackpoint']

    # Main method
    def create_all_collections(self):
        for i in self.collection_names:
            self.create_collection(i)

    def create_collection(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print("Created collection: ", collection)

    def drop_all_collections(self):
        collections = [self.db[i] for i in self.collection_names]
        for coll in collections:
            coll.drop()
            print("Dropped collection: ", coll)

    def show_collections(self):
        collections = self.client[config('DATABASE')].list_collection_names()
        print(collections)
