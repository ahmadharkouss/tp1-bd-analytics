from mrjob import job

class Q3(job.MRJob):
    
    def mapper(self, _, line):
        # Extract relevant fields
        country = line[43:45].strip()
        begin_date = line[82:90]
        end_date = line[91:99]
        station_name = line[13:42].strip()

        # Check if country is not empty
        if country != '':
            # Emit key-value pairs with country code as key
            yield country, (station_name, begin_date, end_date)

    def reducer(self, country, records):
        max_difference = 0
        max_length = 0
        max_station = ''

        for record in records:
            station_name, begin_date, end_date = record

            # Calculate the difference between end and start dates
            date_difference = int(end_date) - int(begin_date)

            # Update max_station if the current station has a larger date difference
            # or has the same date difference but a longer name
            if date_difference > max_difference or (date_difference == max_difference and len(station_name) > max_length):
                max_difference = date_difference
                max_length = len(station_name)
                max_station = station_name

        # Emit the station with the maximum date difference and longest name
        yield country, max_station

if __name__ == '__main__':
    Q3.run()
