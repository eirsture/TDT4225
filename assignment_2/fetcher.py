import os
import pprint


"""
    Data structure:
        self.data:
            {
                000: (user_id)
                    20081023025304: (activity_id, "plt file name")
                        [[trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                    20081024020959:
                        [[trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                    ...
                001: (user_id)
                    ...
                ...
                181: (user_id)
                    ...
            }

"""


class Fetcher:

    def __init__(self):
        self.data = {}

    # Main method: iterates file tree and inserts users, activities and trackpoints
    # Filters out trackpoints with more than 2500 lines (excluding headers)
    def fetch_data(self, num_users=181):
        current_user = ''
        iterations = 0  # Used for testing, iterates number of users indicated by num_users
        for root, dirs, files in os.walk("dataset"):
            if iterations == num_users + 3:  # + 3 here because three first iterations are uninteresting files/dirs
                break
            if "Trajectory" in dirs:
                current_user = root[-3:]
                print("Adding activities and trackpoints for user with id: {}".format(current_user))
                self.data[current_user] = {}  # Create directory for each user

                if files:  # Dataset specific, is used to identify if a user has labeled activities
                    labels_filepath = os.path.join(root, files[0])
                    self.add_labels_to_user(current_user, labels_filepath)

            if current_user:  # Dataset specific, skips uninteresting files at higher directory level
                for name in files:
                    activity_filepath = os.path.join(root, name)
                    self.add_activities_and_trackpoints_to_user(current_user, activity_filepath, name[:-4])
            if files:
                iterations += 1
        return self.data

    # Helper method for fetch_data
    def add_labels_to_user(self, current_user, labels_filepath):
        self.data[current_user]['labels'] = []
        labels_file = open(labels_filepath, "r")
        lines = labels_file.readlines()[1:]
        for labeled_activity in lines:
            self.data[current_user]['labels'].append(labeled_activity.strip().split("\t"))

    # Helper method for fetch_data
    def add_activities_and_trackpoints_to_user(self, current_user, activity_filepath, activity_id):
        activity_file = open(activity_filepath, "r")
        trackpoints = activity_file.readlines()[6:]

        # Ensures that activities with too many trackpoints don't get added
        if sum(1 for trackpoint in trackpoints) > 2500:
            return
        self.data[current_user][activity_id] = []
        for trackpoint in trackpoints:

            self.data[current_user][activity_id].append(trackpoint.strip().split("\t"))

    def get_data(self):
        return self.data


def main():
    fetcher = Fetcher()
    fetcher.fetch_data(2)
    # pprint.pp(fetcher.get_data())


if __name__ == '__main__':
    main()
