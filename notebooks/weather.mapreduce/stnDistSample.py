#!/usr/bin/python
"""
Get the weight distribution of stations in sample_dat.csv
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
from sys import stderr
import re,pickle,base64,zlib

def decode(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def encode(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))

class stnDistSample(MRJob):
    def pca_prescreen_mapper(self,_,line):
        try:
            stn,trecEn = line.split("\t")
            stn=stn.strip('\"')
            #trecEn=trecEn.strip('\"')
            yield stn,1
        except Exception, e:
            stderr.write('Error in line:\n'+str(line[:5]))
            stderr.write(str(e))
    def pca_prescreen_reducer(self,key,value):
        try:
            outkey=key
            outval=sum(list(value))
            if outval>5:
                yield outkey,outval
        except Exception as e:
            stderr.write('Error:')
            stderr.write(str(e)+"\t")
            
    def steps(self):
        return [
            self.mr(mapper=self.pca_prescreen_mapper,
                    reducer=self.pca_prescreen_reducer)
        ]

if __name__ == '__main__':
    stnDistSample.run()