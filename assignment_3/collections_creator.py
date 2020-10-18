from db_connector import DbConnector

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
            self.drop_collection(coll)

    def drop_collection(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()
        print("Dropped collection: ", collection)

    def show_collections(self):
        collections = [self.client[i].list_collection_names() for i in self.collection_names]
        print(collections)

    def show_coll(self, name):
        collections = self.client[name].list_collection_names()
        print(collections)