from mrjob.job import MRJob


class AveragePricePerHour(MRJob):

    def mapper(self, _, line):
        # Split the CSV line into fields
        fields = line.split(';')

        if fields[0].isnumeric():
            # Extract the duration, payment, and start time
            duration = float(fields[5])  # Duration in hours
            payment = float(fields[4])

            # Output key-value pairs with start hour as the key and price per hour as the value
            yield "Prix moyen par heure", (payment, duration)

    def reducer(self, key, values):
        total_payment = 0
        total_duration = 0

        # Aggregate total payment and total duration
        for payment, duration in values:
            total_payment += payment
            total_duration += duration

        # Calculate the average price per hour
        average_price_per_hour = total_payment / total_duration

        # Output the average price per hour for the hour
        yield key, average_price_per_hour


if __name__ == '__main__':
    AveragePricePerHour.run()
