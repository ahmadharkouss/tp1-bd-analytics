from mrjob.job import MRJob
from mrjob.step import MRStep


class MRParkingMeterCost(MRJob):

    def mapper(self, _, line):
        # Split the CSV line into fields
        fields = line.split(';')

        if fields[0].isnumeric():
            # Extract parking meter ID, duration, and revenue generated
            meter_id = fields[0]
            duration = float(fields[5])  # Duration in hours
            revenue = float(fields[4])

            # Calculate cost per hour
            cost_per_hour = revenue / duration if duration > 0 else 0

            # Output key-value pairs with the parking meter ID as the key and cost per hour as the value
            yield meter_id, cost_per_hour

    def reducer(self, meter_id, cost_per_hours):
        # Calculate the average cost per hour for each parking meter
        cost_per_hours_list = list(cost_per_hours)
        total_cost = sum(cost_per_hours_list)
        num_hours = len(cost_per_hours_list)
        average_cost_per_hour = total_cost / num_hours if num_hours > 0 else 0

        # Output the parking meter ID and its average cost per hour
        yield None, (meter_id, average_cost_per_hour)

    def final_reducer(self, _, meter_costs):
        # Sort the parking meters based on the average cost per hour
        sorted_meters = sorted(meter_costs, key=lambda x: x[1])

        # Yield the top 10 and bottom 10 parking meters based on the average cost per hour
        top_10 = sorted_meters[-10:]
        bottom_10 = sorted_meters[:10]

        yield "Top 10 Parking Meters (Highest Average Cost per Hour)", top_10
        yield "Bottom 10 Parking Meters (Lowest Average Cost per Hour)", bottom_10

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]


if __name__ == '__main__':
    MRParkingMeterCost.run()
