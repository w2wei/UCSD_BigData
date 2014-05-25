# sample hdfs:/weather/weather.csv to make a test set.
#!/usr/bin/python
"""
sample ration 1/1000
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr
import random


class sample(MRJob):
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            if random.random()<0.1:
                yield (None,line)
        except Exception, e:
            stderr.write(str(e))
            self.increment_counter('MrJob Counters','mapper-error',1)

if __name__ == '__main__':
    sample.run()