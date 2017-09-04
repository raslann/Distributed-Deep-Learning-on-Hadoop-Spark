#Note to the Grader: I am working with images dataset that already had desciption (i.e, min, max, std). 
#It also has the range which is fixed for all images (0:255)
#one thing I thought it will be useful in my analysis is to know which pixel value occured the most and least. These pixels will have the most/least power in the classification task.
#This program get the max frequent pixel value 

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):

    def mapper_get_words(self, _, line):
        for pixel in WORD_RE.findall(line):
            yield (pixel.lower(), 1)

    def combiner_count_words(self, pixel, counts):
        yield (pixel, sum(counts))

    def reducer_count_words(self, pixel, counts):
        yield None, (sum(counts), pixel)

    def reducer_find_max_word(self, _, pixelValue_count_pairs):
        print ('The most frequent pixel value and its value is:' ,max(pixelValue_count_pairs))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

if __name__ == '__main__':
    MRMostUsedWord.run()
