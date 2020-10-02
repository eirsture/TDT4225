import os

"""
    Data structure:
        self.data:
            {
                000: (user_id)
                    1: (activity_id, "plt file name")
                        [<activity start datetime>, <activity end datetime>, 
                            [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                    2:
                        [<activity start datetime>, <activity end datetime>, 
                            [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                    ...
                001: (user_id)
                    ...
                ...
                181: (user_id)
                    ...
            }
            
        trackpoint example:
            ['40.127951', '116.48646', '0', '62', '39969.065474537', '2009-06-05', '01:34:17']



        self.labels: (see txt file labeled_ids for more info)
            {
                010:
                    [[label 1], [label 2], ..., [label N]]
                020:
                    [[label 1], [label 2], ..., [label N]]
                ...
                179:
                    [[label 1], [label 2], ..., [label N]]
            }
            
        label example:
            ['2007/06/26 11:32:29', '2007/06/26 11:40:29', 'bus']
"""


class Fetcher:

    def __init__(self):
        self.data = {}
        self.labels = {}

    # Main method: iterates file tree and inserts users, activities and trackpoints
    # Filters out trackpoints with more than 2500 lines (excluding headers)
    def fetch_data(self, num_users=181):
        user_id = ''
        activity_id = 0
        iterations = 0  # num_user is unstable, will give some deterministic number of users close to num_users
        for root, dirs, files in os.walk("dataset"):
            if iterations == num_users + 3:  # + 3 here because three first iterations are uninteresting files/dirs
                break
            if "Trajectory" in dirs:
                user_id = root[-3:]
                print("Adding activities and trackpoints for user with id: {}".format(user_id))
                self.data[user_id] = {}  # Create directory for each user

                if files:  # Dataset specific, is used to identify if a user has labeled activities
                    labels_filepath = os.path.join(root, files[0])
                    self.add_labels_to_user(user_id, labels_filepath)

            # Dataset specific, skips uninteresting files at higher directory level
            if user_id and "Trajectory" not in dirs:
                for activity in files:
                    activity_id += 1
                    activity_filepath = os.path.join(root, activity)
                    self.add_activities_and_trackpoints_to_user(user_id, activity_filepath, activity_id)
            if files:
                iterations += 1
        return self.data, self.labels

    # Adds label to user in self.labels, key is user id
    def add_labels_to_user(self, current_user, labels_filepath):
        self.labels[current_user] = []
        labels_file = open(labels_filepath, "r")
        lines = labels_file.readlines()[1:]
        for labeled_activity in lines:
            self.labels[current_user].append(labeled_activity.strip().split("\t"))

    # Helper method for fetch_data
    def add_activities_and_trackpoints_to_user(self, user_id, activity_filepath, activity_id):
        activity_file = open(activity_filepath, "r")
        trackpoints = activity_file.readlines()[6:]

        # Ensures that activities with too many trackpoints don't get added
        if sum(1 for trackpoint in trackpoints) > 2500:
            return
        self.data[user_id][activity_id] = []

        if trackpoints:
            start_date = trackpoints[0].strip().split(",")[5]
            start_time = trackpoints[0].strip().split(",")[6]
            start_date_time = "{} {}".format(start_date, start_time)

            end_date = trackpoints[len(trackpoints)-1].strip().split(",")[5]
            end_time = trackpoints[len(trackpoints)-1].strip().split(",")[6]
            end_date_time = "{} {}".format(end_date, end_time)

            self.data[user_id][activity_id].append(start_date_time)
            self.data[user_id][activity_id].append(end_date_time)

        for trackpoint in trackpoints:
            trackpoint_attributes = trackpoint.strip().split(",")
            self.data[user_id][activity_id].append(trackpoint_attributes)


def main():
    fetcher = Fetcher()
    fetcher.fetch_data(15)


if __name__ == '__main__':
    main()
