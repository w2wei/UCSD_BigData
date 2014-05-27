#!/usr/bin/python
"""
create a sample set for PCA test
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
from sys import stderr
import random
import re,pickle,base64,zlib

def decode(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def encode(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))

class samplePCADat(MRJob):
    def sample_dat_mapper(self,_,line):
        try:
            stn,trecEncode = line.split("\t")
            stderr.write("Line: "+"\t"+stn+"\t"+trecEncode[:10]+"\n")
            if random.random()<0.01:
                yield stn.strip('\"'),trecEncode.strip('\"')
        except Exception, e:
            stderr.write('Error in line:\n'+str(line[:5]))
            stderr.write(str(e))

    def steps(self):
        return [
            self.mr(mapper=self.sample_dat_mapper)
        ]

if __name__ == '__main__':
    samplePCADat.run()