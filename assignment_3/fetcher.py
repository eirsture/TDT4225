import os

"""
    Data structure:
        self.data:
            {
                000: (user_id)
                    1: (activity_id, "plt file name")
                        [<activity start datetime>, <activity end datetime>, <transportation_mode>
                            [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                    2:
                        [<activity start datetime>, <activity end datetime>, <transportation_mode> 
                            [trackpoint 1], [trackpoint 2], ..., [trackpoint N]]
                    ...
                001: (user_id)
                    ...
                ...
                181: (user_id)
                    ...
            }
            
        trackpoint example:
            ['40.127951', '116.48646', '62', '39969.065474537', '2009-06-05 01:34:17']



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
            ['2007-06-26 11:32:29', '2007-06-26 11:40:29', 'bus']
"""


class Fetcher:

    def __init__(self):
        self.data = {}
        self.labels = {}

    # Main method: iterates file tree and inserts users, activities and trackpoints
    # Filters out trackpoints with more than 2500 lines (excluding headers)
    def fetch_data(self):
        user_id = ''
        activity_id = 0
        # iterations = 0  # num_user is unstable, will give some deterministic number of users close to num_users
        for root, dirs, files in os.walk("dataset"):
            #if iterations == num_users + 3:  # + 3 here because three first iterations are uninteresting files/dirs
            #    break
            if "Trajectory" in dirs:
                user_id = root[-3:]
                print("Fetching activities and trackpoints for user with id: {}".format(user_id))
                self.data[user_id] = {}  # Create directory for each user

                if files:  # Dataset specific, is used to identify if a user has labeled activities
                    labels_filepath = os.path.join(root, files[0])
                    self.add_labels_to_user(user_id, labels_filepath)

            # Dataset specific, skips uninteresting files at higher directory level
            if user_id and "Trajectory" not in dirs:
                for activity in files:
                    activity_filepath = os.path.join(root, activity)
                    activity_id += self.add_activities_and_trackpoints_to_user(user_id, activity_filepath, activity_id)
            #if files:
            #    iterations += 1
        return self.data, self.labels

    # Adds label to user in self.labels, key is user id
    def add_labels_to_user(self, current_user, labels_filepath):
        self.labels[current_user] = []
        labels_file = open(labels_filepath, "r")
        lines = labels_file.readlines()[1:]
        for labeled_activity in lines:
            self.labels[current_user].append(labeled_activity.strip().replace("/", "-").split("\t"))

    # Helper method for fetch_data
    def add_activities_and_trackpoints_to_user(self, user_id, activity_filepath, activity_id):
        activity_file = open(activity_filepath, "r")
        trackpoints = activity_file.readlines()[6:]

        # Ensures that activities with too many trackpoints don't get added
        num_trackpoints = sum(1 for trackpoint in trackpoints)
        if num_trackpoints > 2500:
            # print("Activity too large to be added, number of trackpoints: {}, proposed activity id: {}"
            #       .format(num_trackpoints, activity_id+1))
            return 0
        activity_id += 1
        self.data[user_id][activity_id] = []

        start_date = trackpoints[0].strip().split(",")[5]
        start_time = trackpoints[0].strip().split(",")[6]
        start_date_time = "{} {}".format(start_date, start_time)

        end_date = trackpoints[len(trackpoints)-1].strip().split(",")[5]
        end_time = trackpoints[len(trackpoints)-1].strip().split(",")[6]
        end_date_time = "{} {}".format(end_date, end_time)

        transportation_mode = None
        if user_id in self.labels:
            transportation_mode = self.determine_transportation(
                user_id, start_date_time, end_date_time)

        self.data[user_id][activity_id].append(start_date_time)
        self.data[user_id][activity_id].append(end_date_time)
        self.data[user_id][activity_id].append(transportation_mode)

        for trackpoint in trackpoints:
            self.add_trackpoint_to_activity(user_id, activity_id, trackpoint)
        return 1

    def determine_transportation(self, user, start_date_time, end_date_time):
        labeled_activities = self.labels[user]
        for activity in labeled_activities:
            label_activity_start = activity[0]
            label_activity_end = activity[1]
            transportation_mode = activity[2]
            if label_activity_start == start_date_time and label_activity_end == end_date_time:
                return "{}".format(transportation_mode)
        return None

    def add_trackpoint_to_activity(self, user_id, activity_id, trackpoint):
        trckpnt_attr = trackpoint.strip().split(",")  # Trackpoint attributes
        lat, lon, altitude = trckpnt_attr[0], trckpnt_attr[1], trckpnt_attr[3]
        date_days, date_time_date, date_time_time = trckpnt_attr[4], trckpnt_attr[5], trckpnt_attr[6]
        date_time = "{} {}".format(date_time_date, date_time_time)
        relevant_attributes = [lat, lon, altitude, date_days, date_time]
        self.data[user_id][activity_id].append(relevant_attributes)
