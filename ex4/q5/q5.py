from mrjob import job
from mrjob.protocol import JSONValueProtocol


class TemperatureDifference(job.MRJob):

    # Set the output protocol to JSON

    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, line):
        # Split the CSV line
        fields = line.split(',')

        # Check if the line corresponds to the Central Park station and has TMAX or TMIN values
        if len(fields) >= 7 and fields[0] == 'USW00094728' and (fields[2] == 'TMAX' or fields[2] == 'TMIN'):
            date = fields[1]
            value = int(fields[3])

            # Emit key-value pairs with the date as the key and the temperature value and its type as the value
            yield date, (fields[2], value)

    def reducer(self, date, values):
        # Extract TMAX and TMIN values for the same date
        tmax = None
        tmin = None

        for value in values:
            if value[0] == 'TMIN':
                tmin = value[1]
            elif value[0] == 'TMAX':
                tmax = value[1]

        # If both TMAX and TMIN values are present, calculate the temperature difference
        if tmax is not None and tmin is not None:
            #the data is stored by 10ths of a degree. so we divide by 10 to get the actual temperature in celcius
            temperature_difference = (abs(tmax - tmin)) / 10
            yield   None, temperature_difference

if __name__ == '__main__':
    TemperatureDifference.run()
