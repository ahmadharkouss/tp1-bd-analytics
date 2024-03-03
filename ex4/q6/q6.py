from mrjob import job

class TemperatureVariation(job.MRJob):

    def mapper(self, _, line):
        # Split the CSV line
        fields = line.split(',')

        # Check if the line corresponds to the Central Park station and has TMAX or TMIN values
        if len(fields) >= 7 and fields[0] == 'USW00094728' and (fields[2] == 'TMAX' or fields[2] == 'TMIN'):
            date = fields[1]
            value = int(fields[3])

            # Convert temperature to Celsius (divide by 10)
            value = value / 10.0

            # Emit key-value pairs with the date as the key and the temperature value as the value
            yield date, value

if __name__ == '__main__':
    TemperatureVariation.run()
