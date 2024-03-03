from mrjob import job

class TemperatureDifference(job.MRJob):

    def mapper(self, _, line):
        # Split the CSV line
        fields = line.split(',')

        # Check if the line corresponds to the Central Park station and has TMAX or TMIN values
        if len(fields) >= 7 and fields[0] == 'USW00094728' and (fields[2] == 'TMAX' or fields[2] == 'TMIN'):
            date = fields[1]
            value = int(fields[3])

            # Emit key-value pairs with the date as the key and the temperature value as the value
            yield date, value

    def reducer(self, date, values):
        # Extract TMAX and TMIN values for the same date
        tmax = None
        tmin = None

        for value in values:
            if value < 0:
                tmin = abs(value)
            else:
                tmax = value

        # If both TMAX and TMIN values are present, calculate the temperature difference
        if tmax is not None and tmin is not None:
            temperature_difference = tmax - tmin
            yield None, temperature_difference

if __name__ == '__main__':
    TemperatureDifference.run()
