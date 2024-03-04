from mrjob.job import MRJob


class PaymentTotalPerMonthYear(MRJob):

    def mapper(self, _, line):
        # Split the CSV line into fields
        fields = line.split(';')

        if fields[0].isnumeric():
            # Extract year and month from the date field
            year = fields[1][0:4]
            month = fields[1][5:7]

            # Output key-value pairs with year-month as the key and the payment amount as the value
            yield (year, month), float(fields[4])

    def reducer(self, key, values):
        # Sum up all the payment amounts for each year-month key
        total_amount = sum(values)

        # Output the total amount for the year-month
        yield key, total_amount


if __name__ == '__main__':
    PaymentTotalPerMonthYear.run()
