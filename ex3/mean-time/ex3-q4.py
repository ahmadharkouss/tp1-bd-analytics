from mrjob.job import MRJob


class MRAverageParkingDuration(MRJob):

    def mapper(self, _, line):
        # Split the CSV line into fields
        fields = line.split(';')

        if fields[0].isnumeric():
            # Extract user type and parking duration
            user_type = fields[2]
            parking_duration = float(fields[5])

            # Output key-value pairs with the user type as the key and parking duration as the value
            yield user_type, parking_duration

    def reducer(self, user_type, durations):
        total_duration = 0
        num_parkings = 0

        # Calculate total duration and count of parkings for each user type
        for duration in durations:
            total_duration += duration
            num_parkings += 1

        # Calculate the average duration per user type
        if num_parkings > 0:
            average_duration = total_duration / num_parkings
        else:
            average_duration = 0

        # Output the user type and its average parking duration
        yield user_type, average_duration


if __name__ == '__main__':
    MRAverageParkingDuration.run()
