“””
The only cleaning process that may be helpful to pixels is: Normalization 
This Module normalize the pixels values between [0,1].
This would lead to stable training and faster converge.
“””

from mrjob.job import MRJob
import re
import csv
import pandas as pd
import numpy as	np

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
	return row

class normalize(MRJob):

        def mapper(self, _, line):
                pixels = csv_readline(line)
                pixels = map(int, pixels) # cast pixels from string to integer
                pixels = np.divide(pixels, 255)
                yield "pixels", pixels


        def reducer(self, key, values):

                yield "normalized pixels: ", values

if __name__ == '__main__':
    normalize.run()
