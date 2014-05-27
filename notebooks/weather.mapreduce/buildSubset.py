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

def loads(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def dumps(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))
 

class buildSubset(MRJob):
    def collection_mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            rec = line.split(",")
            stn,meas,year = rec[:3]
            if meas=='TMAX' or meas=='TMIN': # this is measurement is about tmax of tmin
                if len(filter(None,rec[3:]))/365.0>0.5: # measurements during >50% days of the year 
                    yield ((stn,year),line)
        except Exception, e:
            stderr.write('Error in line:\n'+str(line[:5]))
            stderr.write(str(e))
            self.increment_counter('MrJob Counters','mapper-error',1)

    def collection_reducer(self, key, value):
        try:
            self.increment_counter('MrJob Counters','reducer',1)
            recs=[v for v in value] # all record for (station, year)
            tmax=[]
            tmin=[]
            for rec in recs:
                stderr.write(key[0]+"\t"+str(len(rec.split(","))))
                recList=rec.split(",")
                if recList[1]=="TMAX":
                    tmax = recList[3:]
                elif recList[1]=="TMIN":
                    tmin = recList[3:]
                else:
                    stderr.write("Error in record: "+str(key))
            res = [tmax,tmin]
            stderr.write("reducer\t"+key[0]+"\t"+str(tmax[:5])+"\t"+str(tmin[:5]))
            yield (key[0], dumps(res))

        except Exception as e:
            stderr.write('Error:')
            stderr.write(str(e)+"\t")
    
#     def fileoutput_reducer(self,key,value):
#         try:
#             yield ()

    def steps(self):
        return [
            self.mr(mapper=self.collection_mapper,
                    reducer=self.collection_reducer)#,
            #self.mr(reducer=self.fileoutput_reducer)
        ]


if __name__ == '__main__':
    buildSubset.run()