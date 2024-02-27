from mrjob import job

class TP2(job.MRJob):
    def mapper(self, _, line):
        # Split the line into words
        words = line.split()
        # Emit each word with a count of 1
        for word in words:
            if word.isalpha():
                # without case sensitivity
                yield  word.upper()[0], 1

    def reducer(self, word, counts):
        # Sum the counts for each word
        yield word, sum(counts)



if __name__ == '__main__':
    #run with a file as input and  output to a file
    TP2.run()

    