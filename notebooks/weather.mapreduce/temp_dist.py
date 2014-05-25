## summarize all TMAX and TMIN measurements across stations
#!/usr/bin/python
"""
summarize the distribution of tmax and tmin
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class temp(MRJob):

    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            if elements[1]=='TMAX':
                out=('tmax',1)
            elif elements[1]=='TMIN':
                out=('tmin',1)
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            out=('error',1)

        finally:
            yield out
            
    def combiner(self, word, counts):
        self.increment_counter('MrJob Counters','combiner',1)
        yield (word, sum(counts))

    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (word, sum(counts))

if __name__ == '__main__':
    temp.run()