#!/usr/bin/python
"""
Given a station ID, yield all availble data using mapreduce
take USC00515000 as an example

"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
from sys import stderr
import pandas as pd
import re,pickle,base64,zlib

def decode(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def encode(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))
 

class concatTminTmax(MRJob):
    def concat_mapper(self, _, line):
        try:
            rec = line.split(",")
            stn,meas,year = rec[:3]
            if meas=='TMAX' or meas=='TMIN': # this is measurement is about tmax of tmin
                if len(filter(None,rec[3:]))/365.0>0.5: # measurements during >50% days of the year 
                    yield stn+":"+year,encode([meas,rec[3:]])
        except Exception, e:
            stderr.write('Error in line:\n'+str(line[:5]))
            stderr.write(str(e))

    def concat_reducer(self, key, value):
        try:
            stn,year=key.split(":")
            recs = list(value)
            trec={}
            if len(recs)==2:
                stderr.write(stn+"\t"+year+"\n")
                for r in recs:
                    meas,vec = decode(r)
                    trec[meas]=vec
                #k1,k2=trec.keys() # for test 
                #stderr.write(stn+"\t"+year+"\t"+k1+"\t"+str(trec[k1][:10])+"\t"+k2+"\t"+str(trec[k2][:10])+"\n")
                yield stn,encode(trec)

        except Exception as e:
            stderr.write('Error:')
            stderr.write(str(e)+"\t")
    

    def steps(self):
        return [
            self.mr(mapper=self.concat_mapper,
                    reducer=self.concat_reducer)
        ]


if __name__ == '__main__':
    concatTminTmax.run()