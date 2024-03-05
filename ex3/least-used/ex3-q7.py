from mrjob.job import MRJob
from mrjob.step import MRStep


class MRParkingLeastUsed(MRJob):

    def mapper(self, _, line):
        # Split the CSV line into fields
        fields = line.split(';')

        if fields[0].isnumeric():
            # Extract parking meter ID and duration of parking
            meter_id = fields[0]
            duration = float(fields[5])  # Duration in hours

            # Output key-value pairs with the parking meter ID as the key and duration as the value
            yield meter_id, duration

    def reducer(self, meter_id, durations):
        # Calculate the total duration of parking for each parking meter
        total_duration = sum(durations)

        # Output the parking meter ID and its total duration of parking
        yield None, (meter_id, total_duration)

    def final_reducer(self, _, meter_durations):
        # Find the parking meter with the least total duration of parking
        least_used_meter = min(meter_durations, key=lambda x: x[1])

        yield "Least Used Parking Meter", least_used_meter

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]


if __name__ == '__main__':
    MRParkingLeastUsed.run()
