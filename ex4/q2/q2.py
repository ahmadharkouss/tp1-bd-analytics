from mrjob import job

class Q2(job.MRJob):
    def mapper(self, _, line):
        #extract the latitude field from the line , starting at offset 57 and with 7 characters
        country=line[43:45].strip() 

        #check if the country is not empty
        if country != '':
            #check if it is 'FR'
            if country == 'FR':
                #extract the latitude field from the line , starting at offset 57 and with 7 characters 
                #extract the altitude field from the line , starting at offset 73 and with 7 characters
                #extract the station name field from the line , starting at offset 13 and with 29 characters
                lt = line[57:64].strip()
                at = line[74:81].strip()
                name = line[13:42].strip()
                # we used or operator  because a station can have a latitude and no altitude info and vice versa
                if(lt != '' or at != ''):
                    yield 'FR', (name,lt,at)


    def reducer(self,_, values):
        # get the list of values
        list_fr_stations = list(values)
         
        # Extract and convert latitude and altitude correctly
        latitude_valid= [(x[0], float(x[1]), x[2]) for x in list_fr_stations  if x[1] != '']
        altitude_valid= [(x[0], x[1], float(x[2])) for x in list_fr_stations  if x[2] != '']

        max_altitude = max(altitude_valid, key=lambda x: x[2])
        min_latitude = min(latitude_valid, key=lambda x: x[1])
        max_latitude = max(latitude_valid, key=lambda x: x[1])
        yield 'La station FR qui a la plus grande altitude est: ', (max_altitude[0], max_altitude[2])
        yield 'La station FR qui se trouve le plus au sud est: ', (min_latitude[0], min_latitude[1])
        yield 'La station FR qui se trouve le plus au nord est: ', (max_latitude[0], max_latitude[1])



if __name__ == '__main__':
    Q2.run()
