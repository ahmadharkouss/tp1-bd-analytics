from mrjob import job
from mrjob.step import MRStep

from mrjob.protocol import JSONValueProtocol


class AverageTemperatureDifference(job.MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol

    # Extract temperature information for each station
    def mapper(self, _, line):
        # Split the CSV line
        fields = line.split(',')

        # Check if the line has the necessary fields
        if len(fields) >= 7 and (fields[2] == 'TMAX' or fields[2] == 'TMIN'):
            station = fields[0]
            date = fields[1]
            value = int(fields[3])

            # Emit key-value pairs with the station as the key and a tuple containing date, temperature type, and value
            yield station, (date, fields[2], value)

    '''
    The primary purpose of a combiner is to perform a local aggregation of the mapper's output, reducing the amount of data that needs to be transferred over the network to the reducer.
    '''
    # Combine temperature information for each unique date
    def combiner(self, station, values):
        # Create a dictionary to store temperature information for each unique date
        date_temp_dict = {}

        for value in values:
            date, temperature_type, temperature_value = value

            # Append temperature information to the list corresponding to the date
            if date not in date_temp_dict:
                date_temp_dict[date] = []
            date_temp_dict[date].append((temperature_type, temperature_value))

        # Yield key-value pairs with the station as the key and a list of tuples containing date and temperature information
        yield station, list(date_temp_dict.items())


    # Calculate the temperature difference for each day of 2013 for each station
    def reducer_first_step (self, station, date_temp_list):

        # Combine dictionaries and convert to the desired list format of tuples (date, temp_list)
        combined_date_temp_list = []

        for date_temp_dict in date_temp_list:
            for date, temp_list in date_temp_dict:
                combined_date_temp_list.append((date, temp_list))

        date_temp_var =[] 
        for date, temp_list in combined_date_temp_list:

            tmax = None
            tmin = None

            for temp_type , temp in temp_list:
                if temp_type == 'TMIN':
                    tmin = temp
                elif temp_type == 'TMAX':
                    tmax = temp
            if tmax is not None and tmin is not None:
                #the data is stored by 10ths of a degree. so we divide by 10 to get the actual temperature in celcius
                temperature_difference = (abs(tmax - tmin)) /10
                date_temp_var.append( (date ,temperature_difference) )

        
        yield station, date_temp_var

    


    # Calculate the average temperature difference/day of 2013 for each station
    def reducer_second_step(self, station, date_temp_var):
        # Flatten the list of lists into a single list of tuples
        flat_date_temp_var = [(date, temp_var) for sublist in date_temp_var for date, temp_var in sublist]

        # Calculate the average temperature difference for each station
        if(len(flat_date_temp_var) != 0):
            average_temperature_difference = sum(temp_var for date, temp_var in flat_date_temp_var) / len(flat_date_temp_var)
            # Yield key-value pairs with the station as the key and the average temperature difference as the value
            yield None, average_temperature_difference
            
    def steps(self):
        return [
            MRStep(mapper=self.mapper,combiner=self.combiner , reducer=self.reducer_first_step),
            MRStep(reducer=self.reducer_second_step)
        ]

if __name__ == '__main__':
    AverageTemperatureDifference.run()

