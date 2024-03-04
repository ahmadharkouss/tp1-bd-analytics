from mrjob.job import MRJob
from mrjob.step import MRStep


class MRParkingMeterRevenue(MRJob):

    def mapper(self, _, line):
        # Split the CSV line into fields
        fields = line.split(';')

        if fields[0].isnumeric():
            # Extract parking meter ID and revenue generated
            meter_id = fields[0]
            revenue = float(fields[4])

            # Output key-value pairs with the parking meter ID as the key and revenue as the value
            yield meter_id, revenue

    def reducer(self, meter_id, revenues):
        total_revenue = sum(revenues)

        # Output the parking meter ID and its total revenue
        yield None, (meter_id, total_revenue)

    def final_reducer(self, _, meter_revenues):
        # Find the parking meter with the highest revenue
        max_revenue_meter = max(meter_revenues, key=lambda x: x[1])

        # Output the parking meter with the highest revenue
        yield "Parking Meter with Most Revenue", max_revenue_meter


    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]


if __name__ == '__main__':
    MRParkingMeterRevenue.run()
