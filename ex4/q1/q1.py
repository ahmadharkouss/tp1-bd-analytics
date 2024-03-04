from mrjob import job

class Q1(job.MRJob):
    def mapper(self, _, line):
        #extract the latitude field from the line , starting at offset 57 and with 7 characters
        lt = line[57:64].strip() 
        #log the latitude
        #check ig the latitude is empty
        if lt == '':
            #if it is, emit a count of 1 for the hemisphere 'empty'
            yield 'le # total de stations avec des valeurs nulles en latitude', 1
        elif float(lt) >= 0 and float(lt) <= 90: 
            #if it is, emit a count of 1 for the hemisphere 'north'
            yield 'le # total de stations en hemisphere nord', 1
        elif float(lt) <= 0 and float(lt) >= -90:
            #if it is, emit a count of 1 for the hemisphere 'south'
            yield 'le # total de stations in hemisphere sud', 1
    def reducer(self, key, counts):
        # Sum the counts for each word
        yield key, sum(counts)

if __name__ == '__main__':
    Q1.run()
