from mrjob.job import MRJob
from mrjob.step import MRStep


class MRPaymentProportion(MRJob):

    def mapper(self, _, line):
        # Split the CSV line into fields
        fields = line.split(';')

        if fields[0].isnumeric():
            # Extract the payment method
            payment_method = fields[3]

            # Output key-value pairs with the payment method as the key and count as the value
            yield payment_method, 1

    def reducer(self, key, values):
        # Sum up the counts for each payment method
        total_count = sum(values)

        # Output the total count for the payment method
        yield None, (key, total_count)

    def final_reducer(self, key, values):
        # Initialize variables to store counts for Paris Carte and CB transactions
        paris_carte_count = 0
        cb_count = 0

        # Iterate through the values and update counts
        for method, count in values:
            if method == 'Paris Carte':
                paris_carte_count += count
            elif method == 'CB':
                cb_count += count

        # Calculate the proportion of Paris Carte transactions compared to CB transactions
        proportion = paris_carte_count / cb_count if cb_count != 0 else 0

        # Output the proportion
        yield 'Proportion of Paris Carte on CB', proportion

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]


if __name__ == '__main__':
    MRPaymentProportion.run()
