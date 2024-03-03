from mrjob.job import MRJob
import string


class MRPalindromeFinder(MRJob):

    def mapper(self, _, line):
        # Split the line into words and remove punctuation
        words = line.translate(str.maketrans('', '', string.punctuation)).split()
        # Emit each valid word sorted alphabetically as the key, and the original word as the value
        for word in words:
            # Check if the word has more than one letter and consists only of alphabetic characters
            if len(word) > 1 and word.isalpha():
                # Normalize the word to lowercase for case-insensitive comparison
                if self.is_palindrome(word.lower()):
                	yield word.lower(), None  # Emit lowercase version of the word as key

    def reducer(self, key, values):
        yield key, None

    def is_palindrome(self, word):
        # Check if the word is a palindrome
        return word == word[::-1]


if __name__ == '__main__':
    MRPalindromeFinder.run()

