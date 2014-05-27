#!/usr/bin/python
"""
PCA on a single station
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
 

class mrpca(MRJob):
    def pca_mapper(self, _, line):
        try:
            stn,rawDat = [x.strip('\"') for x in line.split("\t")]
            stderr.write(stn+"\t"+str(decode(rawDat))+"\n") # 
            datDict = decode(rawDat)
            datArr = np.array([datDict["TMAX"],datDict["TMIN"]])
            stderr.write("dataArr "+str(datArr.shape))
            
            yield stn, encode(datArr)
        except Exception, e:
            stderr.write('Error in line:\n'+str(line[:5]))
            stderr.write(str(e))

#     def concat_reducer(self, key, value):
#         try:
#             stn,year=key.split(":")
#             recs = list(value)
#             trec={}
#             if len(recs)==2:
#                 stderr.write(stn+"\t"+year+"\n")
#                 for r in recs:
#                     meas,vec = decode(r)
#                     trec[meas]=vec
#                 #k1,k2=trec.keys() # for test 
#                 #stderr.write(stn+"\t"+year+"\t"+k1+"\t"+str(trec[k1][:10])+"\t"+k2+"\t"+str(trec[k2][:10])+"\n")
#                 yield stn,encode(trec)

#         except Exception as e:
#             stderr.write('Error:')
#             stderr.write(str(e)+"\t")
    

    def steps(self):
        return [
            self.mr(mapper=self.pca_mapper)
        ]


if __name__ == '__main__':
    mrpca.run()