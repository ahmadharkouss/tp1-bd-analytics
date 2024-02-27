from mrjob.job import MRJob
from collections import defaultdict
import string
import itertools


class MRAnagramFinder(MRJob):

    def mapper(self, _, line):
        # Split the line into words and remove punctuation
        words = line.translate(str.maketrans('', '', string.punctuation)).split()
        # Emit each valid word sorted alphabetically as the key, and the original word as the value
        for word in words:
            # Check if the word has more than one letter and consists only of alphabetic characters
            if len(word) > 1 and word.isalpha():
                # Normalize the word to lowercase for case-insensitive comparison
                sorted_word = ''.join(sorted(word.lower()))
                yield sorted_word, word.lower()  # Emit lowercase version of the word as value

    def reducer(self, key, values):
        # Collect all words that are anagrams of each other, ignoring case
        anagrams = set()
        for word in values:
            anagrams.add(word)
        # Only emit anagrams that have more than one word
        if len(anagrams) > 1:
            yield key, list(anagrams)


if __name__ == '__main__':
    MRAnagramFinder.run()

