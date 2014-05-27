#!/usr/bin/python
"""
create a sample set for PCA test
"""

import random


class samplePCADat(MRJob):
    def sample_dat_mapper(self,_,line):
        try:
            stn,trecEncode = line.split("\t")
#             stderr.write("Line: "+"\t"+stn+"\t"+trecEncode[:10]+"\n")
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