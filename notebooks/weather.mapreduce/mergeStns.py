#!/usr/bin/python
"""
Merge neighbor nodes according to the number of selected eigen vectors
"""
from mrjob.job import MRJob
from sys import stderr
import re,pickle,base64,zlib

def decode(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def encode(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))

def treeInfo():
    tree = pickle.load(open('/home/ubuntu/UCSD_BigData/data/weather/tree.pkl'))
    ptree = tree['Partition_Tree']
    pstn = tree['Partitioned_Stations']
    pstnSample=pstn[pstn['Node']=='000000000']
    pstnSample.sort(['latitude','longitude']) # sort by both latitude and longitude, latitude prior
    ## select the first two stations
    stn_1 = {"ID":pstnSample.index[0],'wt':pstnSample.loc[pstnSample.index[0],'weight']}
    stn_2 = {"ID":pstnSample.index[1],'wt':pstnSample.loc[pstnSample.index[1],'weight']}
    stderr.write("stn1"+"\n\t"+str(stn_1))
    return [stn_1,stn_2]

class mergeStns(MRJob):
    '''
    List all stations belonging to one node, such as 000000000
    calculate every pair of stations and exame the description length.
    If the description length after merging is smaller than the sum of the numbers 
    of original eigen vectors, the two stations will be merged. 
    This can be generalized to merging nodes.
    '''
    def dat_prep_mapper(self, _, line):
        '''
        Take node 000000000 as an example, find all stations belong to this node; retrieve TMAX/TMIN data of these nodes;
        retrieve eigen vectors of these nodes;
        '''
        try:
            ## use Professor's prebuilt tree
            stn1,stn2 = treeInfo()
            stderr.write("ID: "+stn1['ID']+"\t"+stn2['ID']+"\n")
            stderr.write("***********")
            yield 1,1
        
        except Exception, e:
            stderr.write('Error in line:\n'+str(line[:5]))
            stderr.write(str(e))
            
    def steps(self):
        return [
            self.mr(mapper=self.dat_prep_mapper)
#                     reducer=self.pca_indi_sum),
#             self.mr(reducer=self.pca_indi_mean),
#             self.mr(reducer=self.pca_indi_cov),
#             self.mr(reducer=self.pca_indi_svd)
            
        ]


if __name__ == '__main__':
    mergeStns.run()